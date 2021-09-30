<?php

return [
   'processors' => [
      [
         /*
         | Optional, defaults to `null`
         | Here you may specify which topic should be handled by this processor.
         | Processor handles all topics by default.
         */
         'topic' => 'stage.crm.fact.registrations.1',

         /*
         | Optional, defaults to `null`
         | Here you may specify which greensight/laravel-phprdkafka consumer should be handled by this processor.
         | Processor handles all consumers by default.
         */
         'consumer' => 'default', // optional, processor fits all consumers by default

         /*
         | Optional, defaults to `action`
         | Here you may specify processor's type. Defaults to `action`
         | Supported types:
         |  - `action` - a simple class with execute method;
         |  - `job` - Laravel Queue Job. It will be dispatched using `dispatch` helper;
         */
         'type' => 'action',

         /*
         | Required.
         | Fully qualified class name of a processor class.
         */
         'class' => \App\Domain\Communication\Actions\SendConfirmationEmailAction::class,

         /*
         | Optional, defaults to `false`
         | Supported values:
         |  - `false` - do not stream message. Execute processor in syncronous mode;
         |  - `true` - stream message to Laravel's default queue;
         |  - `<your-favorite-queue-name-as-string>` - stream message to this queue;
         */
         'queue' => false, // optional `true/false/string` to specify a Laravel's queue to stream message to. Defaults to `false`.

         /*
         | Optional, defaults to 5000.
         | Kafka consume timeout in milliseconds .
         */
         'consume_timeout' => 5000,
      ]
   ],
];
