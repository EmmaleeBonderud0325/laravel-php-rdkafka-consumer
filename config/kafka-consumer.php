<?php

return [
   'global_middleware' => [],

   'processors' => [],
   
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
