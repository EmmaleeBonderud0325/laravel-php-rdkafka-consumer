<?php

use Ensi\LaravelPhpRdKafkaConsumer\Consumers\Factories\ConsumerFactory;
use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerProcessorException;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\TestConsumer;

test('get exception when topic key config not found', function () {
    (new ConsumerFactory(resolve(HighLevelConsumer::class)))
        ->build('not-found-topic-key');
})->throws(KafkaConsumerProcessorException::class, 'Processor for topic-key "not-found-topic-key" and consumer "default" is not found');

test('get exception when processor class not found', function () {
    setConsumerTopicConfig('test-models', 'NotFoundConsumerClass');

    (new ConsumerFactory(resolve(HighLevelConsumer::class)))
        ->build('test-models');
})->throws(KafkaConsumerProcessorException::class, 'Processor class "NotFoundConsumerClass" is not found');

test('get exception when processor invalid type', function () {
    setConsumerTopicConfig('test-models', TestConsumer::class, 'invalid-type');

    (new ConsumerFactory(resolve(HighLevelConsumer::class)))
        ->build('test-models');
})->throws(KafkaConsumerProcessorException::class, 'Invalid processor type "invalid-type", supported types are: action,job');

test('set consume_timeout and middleware to consumer options', function () {
    config()
        ->set('kafka-consumer.consumer_options.default', [
            'consume_timeout' => 55000,
            'middleware' => ['AlreadyAddedMiddleware', 'KafkaConsumerMiddleware'],
        ]);

    config()
        ->set('kafka-consumer.global_middleware', ['AlreadyAddedMiddleware']);

    setConsumerTopicConfig('test-models', TestConsumer::class);

    $consumer = (new ConsumerFactory(resolve(HighLevelConsumer::class)))
        ->build('test-models');

    $consumerOptions = $consumer->getConsumerOptions();

    expect($consumerOptions->consumeTimeout)
        ->toBe(55000)
        ->and($consumerOptions->middleware)
        ->toBe(['AlreadyAddedMiddleware', 'KafkaConsumerMiddleware']);
});
