<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests;

use Ensi\LaravelPhpRdKafka\LaravelPhpRdKafkaServiceProvider;
use Ensi\LaravelPhpRdKafkaConsumer\LaravelPhpRdKafkaConsumerServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [
            LaravelPhpRdKafkaConsumerServiceProvider::class,
            LaravelPhpRdKafkaServiceProvider::class,
        ];
    }
}
