package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

class TransientHttpException(val code: Int, message: String?) : Exception(message)

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        private val emptyBody = RequestBody.create(null, ByteArray(0))

        private val mapper = ObjectMapper().registerKotlinModule()

        private val retryableHttpCodes = setOf(429, 500, 502, 503, 504)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private var rateLimiter: RateLimiter = SlidingWindowRateLimiter(10, Duration.ofSeconds(1))
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (System.currentTimeMillis() >= deadline) {
            logger.warn("[$accountName] Parallel requests limit timeout for payment $paymentId. Aborting external call.")
            paymentESService.update(paymentId) {
                it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            return
        }

        while (true) {
            val windowResponse = ongoingWindow.putIntoWindow()
            if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                break
            }
        }

        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        val delay = 1000L
        val maxRetries = 3
        var attempt = 0
        var success = false
        var responseBody: ExternalSysResponse? = null
        var lastException: Exception? = null

        try {
            while (attempt < maxRetries && !success) {
                try {
                    while (!rateLimiter.tick()) {
                    }

                    if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                        return
                    }

                    client.newCall(request).execute().use { response ->
                        val code = response.code

                        if (code in retryableHttpCodes) {
                            throw TransientHttpException(code, "Received HTTP $code from server")
                        }

                        if (code !in 200..299) {
                            logger.error("[$accountName] Non-retryable HTTP code $code for txId: $transactionId, payment: $paymentId")
                            responseBody = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }
                        }

                        responseBody = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        success = true
                    }
                } catch (e: TransientHttpException) {
                    lastException = e
                    attempt++
                    if (attempt < maxRetries) {
                        logger.warn("[$accountName] Attempt #$attempt failed with code ${e.code} for payment $paymentId. Retrying...", e)
                        Thread.sleep(delay)
                    }
                } catch (e: SocketTimeoutException) {
                    lastException = e
                    attempt++
                    if (attempt < maxRetries) {
                        logger.warn("[$accountName] Attempt #$attempt SocketTimeout for payment $paymentId. Retrying...", e)
                        Thread.sleep(delay)
                    }
                } catch (e: Exception) {
                    lastException = e
                    logger.error("[$accountName] Non-retryable exception for txId: $transactionId, payment: $paymentId", e)
                    break
                }
            }

            if (!success) {
                when (lastException) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", lastException)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                    is TransientHttpException -> {
                        logger.error("[$accountName] Max retries exceeded with transient code ${lastException.code} for txId: $transactionId, payment: $paymentId", lastException)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Max retries exceeded for transient error ${lastException.code}.")
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", lastException)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = lastException?.message)
                        }
                    }
                }
                return
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${responseBody?.result}, message: ${responseBody?.message}")

            paymentESService.update(paymentId) {
                it.logProcessing(responseBody?.result ?: false, now(), transactionId, reason = responseBody?.message)
            }
        } finally {
            ongoingWindow.releaseWindow()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
