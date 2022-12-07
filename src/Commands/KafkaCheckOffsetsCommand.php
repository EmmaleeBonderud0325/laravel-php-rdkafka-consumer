<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Commands;

use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Illuminate\Console\Command;

class KafkaCheckOffsetsCommand extends Command
{
    protected $signature = 'kafka:check-offsets';
    protected $description = 'Проверить что для всех используемых топиков задан offset';

    public function handle(): int
    {
        $configuredTopics = config('kafka-consumer.processors', []);
        $success = true;
        foreach ($configuredTopics as $configuredTopic) {
            $success &= $this->checkTopic($configuredTopic['consumer'], $configuredTopic['topic']);
            sleep(1);
        }


        return $success ? self::SUCCESS : self::FAILURE;
    }

    protected function checkTopic(string $consumerName, string $topicName): bool
    {
        $this->getOutput()->writeln("<bg=cyan>Check topic:</> {$topicName}");
        /** @var HighLevelConsumer $consumer */
        $consumer = resolve(HighLevelConsumer::class);
        $consumer->for($consumerName);
        $partitions = $consumer->getPartitions($topicName);
        if (!$partitions) {
            $this->getOutput()->writeln("    <fg=red>Topic {$topicName} doesn't exists!</>");
            return false;
        }
        $success = true;
        foreach ($partitions as $partition) {
            if ($partition->getOffset() < 0) {
                $success = false;
                $this->getOutput()->writeln("    <fg=red>No offset for partition: {$partition->getPartition()}</>");
            }
        }

        if ($success) {
            $this->getOutput()->writeln("    <fg=green>No errors</>");
        }

        return $success;
    }
}
