<?php

use Ensi\LaravelPhpRdKafkaConsumer\Loggers\ConsumerLogger;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;

test('merging context when call log', function () {
    $logger = mock(NullLogger::class)
        ->makePartial();

    $consumerLogger = new ConsumerLogger($logger, 'products', 'default');
    $consumerLogger->log(LogLevel::NOTICE, 'Test message', ['foo' => 'bar']);

    $logger->shouldHaveReceived('log', function ($level, $message, array $context = []) {
        return $level == LogLevel::NOTICE
            && $message === 'Test message'
            && count($context) == 3
            && $context['topic_key'] === 'products'
            && $context['consumer_name'] === 'default'
            && $context['foo'] == 'bar';
    });
});
