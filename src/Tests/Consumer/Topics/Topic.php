<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\Topics;

class Topic
{
    protected array $partitions = [];

    public function __construct(
        protected string $topicName
    ) {
        $this->partitions[] = new Partition();
    }

    public function getTopic(): string
    {
        return $this->topicName;
    }

    public function getPartitions(): array
    {
        return $this->partitions;
    }
}
