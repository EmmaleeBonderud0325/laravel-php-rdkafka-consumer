<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\Topics;

class Metadata
{
    protected array $topics = [];

    public function __construct(string $topicName)
    {
        $this->topics[] = new Topic($topicName);
    }

    public function getTopics(): array
    {
        return $this->topics;
    }
}
