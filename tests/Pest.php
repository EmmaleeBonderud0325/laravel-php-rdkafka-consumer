<?php

use Ensi\LaravelPhpRdKafkaConsumer\Tests\TestCase;

uses(TestCase::class)
    ->in(__DIR__);


function setConsumerTopicConfig(string $topicKey, string $class, string $type = 'action'): void
{
    config()->set('kafka-consumer.processors', [['topic' => $topicKey, 'class' => $class, 'type' => $type]]);
    config()->set('kafka.connections.default.topics', [$topicKey => "production.test.fact.$topicKey.1"]);
}
