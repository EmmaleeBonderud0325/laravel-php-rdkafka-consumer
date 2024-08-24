<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer;

use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerMessagedEndedException;
use Ensi\LaravelPhpRdKafkaConsumer\Tests\Consumer\Topics\Metadata;
use RdKafka\Conf;
use RdKafka\KafkaConsumer as BaseKafkaConsumer;
use RdKafka\TopicConf;

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

    public function getMetadata($all_topics, $only_topic, $timeout_ms): Metadata
    {
        return $this->metadata;
    }

    public function getCommittedOffsets($topicPartitions, $timeout_ms): array
    {
        return $topicPartitions;
    }

    public function newTopic($topic_name, TopicConf $topic_conf = null)
    {

    }

    public function commitAsync($message_or_offsets = null)
    {
    }
}
