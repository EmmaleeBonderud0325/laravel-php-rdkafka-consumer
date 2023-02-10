<?php

namespace Ensi\LaravelPhpRdKafkaConsumer;

class ProcessorData
{
    protected array $supportedTypes = ['action', 'job'];

    public function __construct(
        public string $class,
        public ?string $topicKey = null,
        public ?string $consumer = null,
        public string $type = 'action',
        public string|bool $queue = false,
        /** @deprecated, use `consumer_options` */
        public int $consumeTimeout = 20000,
    ) {
    }

    public function hasValidType(): bool
    {
        return in_array($this->type, $this->supportedTypes);
    }

    public function getSupportedTypes(): array
    {
        return $this->supportedTypes;
    }
}
