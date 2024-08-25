<?php

use Ensi\LaravelPhpRdKafkaConsumer\Loggers\ConsumerLoggerFactory;
use Psr\Log\NullLogger;

test('test create consumer logger from factory', function (?string $channel) {
    config()->set('kafka-consumer.log_channel', $channel);

    $consumerLogger = (new ConsumerLoggerFactory())
        ->make('products', 'default');

    expect($consumerLogger->getLogger())
        ->toBeInstanceOf(NullLogger::class)
        ->and($consumerLogger->getTopicKey())
        ->toBe('products')
        ->and($consumerLogger->getConsumerName())
        ->toBe('default');
})->with([null, 'null']);
