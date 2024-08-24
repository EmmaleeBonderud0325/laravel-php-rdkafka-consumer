<?php

use Ensi\LaravelPhpRdKafkaConsumer\Commands\KafkaConsumeCommand;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\KafkaManagerFaker;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\TestConsumer;
use function Pest\Laravel\artisan;

use RdKafka\Message;

test('consume command test', function () {
    TestConsumer::fake('test-model');

    KafkaManagerFaker::new('test-model')
        ->addMessage($message = new Message())
        ->bind();

    artisan(KafkaConsumeCommand::class, ['topic-key' => 'test-model'])
        ->expectsOutputToContain('Start listening to topic: "test-model" (production.test.fact.test-model.1), consumer "default"')
        ->assertOk();

    TestConsumer::assertMessageConsumed($message);
});
