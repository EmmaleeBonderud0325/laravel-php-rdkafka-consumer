<?php

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
