<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Commands;

use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Illuminate\Console\Command;
use RdKafka\Exception as RdKafkaException;

class KafkaSetOffset extends Command
{
    public const EARLIEST = 'earliest';
    public const LATEST = 'latest';

    protected $signature = 'kafka:set-offset
                            {--consumer= : name of consumer}
                            {--topic= : name of topic}
                            {--partition= : id of partition}
                            {--offset= : desired offset, "earliest", "latest" or positive integer}';
    protected $description = "Задать смещение для консюмера в топике";

    public function handle()
    {
        $consumerName = $this->option('consumer');
        $topicName = $this->option('topic');
        $partitionId = $this->option('partition');
        $offset = $this->option('offset');

        if (!in_array($offset, [self::EARLIEST, self::LATEST]) && !is_numeric($offset)) {
            $this->getOutput()->writeln("<fg=red>Error: Invalid offset</>");
            return self::INVALID;
        }

        /** @var HighLevelConsumer $consumer */
        $consumer = resolve(HighLevelConsumer::class);
        $consumer->for($consumerName);

        try {
            $bounds = $consumer->getPartitionBounds($topicName, $partitionId);
            $realOffset = match($offset) {
                'earliest' => $bounds[0] ?? 0,
                'latest' => $bounds[1] ?? 0,
                default => $offset,
            };

            if ($offset != $realOffset) {
                $this->getOutput()->writeln("<fg=yellow>Use {$offset} offset:</> {$realOffset}");
            }

            $consumer->setOffset($topicName, $partitionId, $realOffset);
        } catch (RdKafkaException $e) {
            $this->getOutput()->writeln("<fg=red>Error: {$e->getMessage()}</>");

            return self::INVALID;
        }
        $this->getOutput()->writeln("<fg=green>Success</>");

        return self::SUCCESS;
    }
}
