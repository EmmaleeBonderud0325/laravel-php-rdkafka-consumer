<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer;

use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerMessagedEndedException;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\Topics\Metadata;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\Topics\Topic;
use RdKafka\Conf;
use RdKafka\KafkaConsumer as BaseKafkaConsumer;

class KafkaConsumer extends BaseKafkaConsumer
{
    protected Metadata $metadata;

    public function __construct(string $topicName, protected array $messages = [])
    {
        parent::__construct($this->makeConf());

        $this->metadata = new Metadata($topicName);
    }

    private function makeConf(): Conf
    {
        $conf = new Conf();
        $conf->set('group.id', 'test');

        return $conf;
    }

    /**
     * @throws KafkaConsumerMessagedEndedException
     */
    public function consume($timeout_ms)
    {
        if (count($this->messages) == 0) {
            throw new KafkaConsumerMessagedEndedException();
        }

        return array_shift($this->messages);
    }

    /**
     * @phpstan-ignore-next-line
     */
    public function getMetadata($all_topics, $only_topic = null, $timeout_ms): Metadata
    {
        return $this->metadata;
    }

    public function getCommittedOffsets($topic_partitions, $timeout_ms): array
    {
        return $topic_partitions;
    }

    /**
     * @phpstan-ignore-next-line
     */
    public function newTopic($topic_name, $topic_conf = null): Topic
    {
        return new Topic($topic_name);
    }

    public function commitAsync($message_or_offsets = null)
    {
    }
}
