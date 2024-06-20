<?php

use _PHPStan_01e5828ef\Psr\Log\LogLevel;
use Ensi\LaravelPhpRdKafkaConsumer\Logger\ConsumerLogger;
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
            && $context['consumer'] === 'default'
            && $context['foo'] == 'bar';
    });
});
