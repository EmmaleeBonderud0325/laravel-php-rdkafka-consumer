<?php

namespace Greensight\LaravelPhpRdKafkaConsumer;

use RdKafka\Message;

abstract class AbstractKafkaJob
{
    public function __construct(protected Message $message)
    {
    }

    abstract public function handle(): void;
}
