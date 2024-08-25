<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Consumers;

use Ensi\LaravelPhpRdKafkaConsumer\ConsumerOptions;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\ProcessorData;
use RdKafka\Exception;
use Throwable;

class Consumer
{
    public function __construct(
        protected HighLevelConsumer $highLevelConsumer,
        protected ProcessorData $processorData,
        protected ConsumerOptions $consumerOptions,
        protected string $topicName
    ) {
    }

    public function getTopicName(): string
    {
        return $this->topicName;
    }

    public function setMaxTime(int $maxTime = 0): self
    {
        $this->consumerOptions->maxTime = $maxTime;

        return $this;
    }

    public function setMaxEvents(int $maxEvents = 0): self
    {
        $this->consumerOptions->maxEvents = $maxEvents;

        return $this;
    }

    public function forceStop(): void
    {
        $this->highLevelConsumer->forceStop();
    }

    /**
     * @return ProcessorData
     */
    public function getProcessorData(): ProcessorData
    {
        return $this->processorData;
    }

    /**
     * @return ConsumerOptions
     */
    public function getConsumerOptions(): ConsumerOptions
    {
        return $this->consumerOptions;
    }

    /**
     * @return void
     *
     * @throws Exception
     * @throws Throwable
     */
    public function listen(): void
    {
        $this->highLevelConsumer
            ->for($this->processorData->consumer)
            ->listen($this->topicName, $this->processorData, $this->consumerOptions);
    }
}
