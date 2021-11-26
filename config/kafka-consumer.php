<?php

return [
   'global_middleware' => [],

   'processors' => [
      [
         /*
         | Optional, defaults to `null`.
         | Here you may specify which topic should be handled by this processor.
         | Processor handles all topics by default.
         */
         'topic' => 'stage.crm.fact.registrations.1',

         /*
         | Optional, defaults to `null`.
         | Here you may specify which ensi/laravel-phprdkafka consumer should be handled by this processor.
         | Processor handles all consumers by default.
         */
         'consumer' => 'default',

         /*
         | Optional, defaults to `action`.
         | Here you may specify processor's type. Defaults to `action`
         | Supported types:
         |  - `action` - a simple class with execute method;
         |  - `job` - Laravel Queue Job. It will be dispatched using `dispatch` or `dispatchSync` method;
         */
         'type' => 'action',

         /*
         | Required.
         | Fully qualified class name of a processor class.
         */
         'class' => \App\Domain\Communication\Actions\SendConfirmationEmailAction::class,

         /*
         | Optional, defaults to `false`.
         | Proxy messages to Laravel's queue.
         | Supported values:
         |  - `false` - do not stream message. Execute processor in syncronous mode;
         |  - `true` - stream message to Laravel's default queue;
         |  - `<your-favorite-queue-name-as-string>` - stream message to this queue;
         */
         'queue' => false,
      ],
   ],
   
   'consumer_options' => [
      /** options for consumer with name `default` */
      'default' => [
         /*
         | Optional, defaults to 20000.
         | Kafka consume timeout in milliseconds.
         */
        'consume_timeout' => 20000,

         /*
         | Optional, defaults to empty array.
         | Array of middleware.
         */
        'middleware' => [],
      ]
   ]
];
