<?php

namespace Greensight\LaravelPhpRdKafkaConsumer\Commands;

use Greensight\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Throwable;
use Illuminate\Console\Command;

class KafkaConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'kafka:consume {topic} {consumer=default}';

    /**
     * The console command description.
     */
    protected $description = 'Consume concrete topic';

    /**
     * Execute the console command.
     */
    public function handle(): int
    {
        $topic = $this->argument('topic');
        $consumer = $this->argument('consumer');
        $availableConsumers = array_keys(config('kafka.consumers', []));

        if (!in_array($consumer, $availableConsumers)) {
            $this->error("Unknown consumer \"$consumer\"");
            $this->line('Available consumers are: "' . implode(', ', $availableConsumers) . '" and can be found in /config/kafka.php');

            return 1;
        }

        $processorData = $this->findMatchedProcessor($topic, $consumer);
        if (is_null($processorData)) {
            $this->error("Processor for topic \"$topic\" and consumer \"$consumer\" is not found");
            $this->line('Processors are set in /config/kafka-consumers.php');

            return 1;
        }

        $processorClassName = $processorData['class'];
        if (!class_exists($processorClassName)) {
            $this->error("Processor class \"$processorClassName\" is not found");
            $this->line('Processors are set in /config/kafka-consumers.php');

            return 1;
        }

        $consumeTimeout = $processorData['consume_timeout'] ?? 5000;

        $supportedProcessorTypes = ['action', 'job'];
        $processorType = $processorData['type'] ?? 'action';
        if (!in_array($processorType, $supportedProcessorTypes)) {
            $this->error("Invalid processor type \"$processorType\", supported types are: " . implode(',', $supportedProcessorTypes));

            return 1;
        }

        $processorQueue = $processorData['queue'] ?? false;

        $this->info("Start listenning to topic: \"$topic\", consumer \"$consumer\"");
        try {
            $kafkaTopicListener = new HighLevelConsumer($topic, $consumer, $consumeTimeout);
            $kafkaTopicListener->listen($processorClassName, $processorType, $processorQueue);
        } catch (Throwable $e) {
            $this->error('An error occurred while listening to the topic: '. $e->getMessage(). ' '. $e->getFile() . '::' . $e->getLine());

            return 1;
        }

        return 0;
    }

    protected function findMatchedProcessor(string $topic, string $consumer): ?array
    {
        foreach (config('kafka-consumer.processors', []) as $processor) {
            if (
                (empty($processor['topic']) || $processor['topic'] === $topic)
                && (empty($processor['consumer']) || $processor['consumer'] === $consumer)
                ) {
                return $processor;
            }
        }

        return null;
    }
}