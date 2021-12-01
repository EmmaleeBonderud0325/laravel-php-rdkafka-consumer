<?php

namespace Ensi\LaravelPhpRdKafkaConsumer;

class ConsumerOptions
{
    public function __construct(
        public int $consumeTimeout = 20000,
        public int $maxEvents = 0,
        public int $maxTime = 0,
        public array $middleware = [],
    ) {
    }
}
