<?php

use Ensi\LaravelPhpRdKafka\KafkaManager;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\ConsumerFaker;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\TestConsumer;
use RdKafka\Message;

test('create and consume kafka manager fake', function () {
    TestConsumer::fake('test-model');

    ConsumerFaker::new('test-model')
        ->addMessage($message = new Message())
        ->consume();

    TestConsumer::assertMessageConsumed($message);
});

test('bind testing kafka manager with faker consumer', function () {
    TestConsumer::fake('test-model');

    ConsumerFaker::new('test-model')
        ->addMessage(new Message())
        ->bind();

    expect(resolve(KafkaManager::class))
        ->toBeInstanceOf(\Ensi\LaravelPhpRdKafkaConsumer\Tests\KafkaManager::class);
});
