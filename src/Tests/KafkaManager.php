<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests;

use Ensi\LaravelPhpRdKafka\KafkaManager as BaseKafkaManager;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\KafkaConsumer;
use RdKafka\KafkaConsumer as BaseKafkaConsumer;

class KafkaManager extends BaseKafkaManager
{
    public function __construct(
        protected KafkaConsumer $kafkaConsumer
    ) {
        parent::__construct();
    }

    public function consumer(string $name = 'default'): BaseKafkaConsumer
    {
        return $this->kafkaConsumer;
    }
}
