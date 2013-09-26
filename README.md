# dispatcher

Dispatcher is a simple library that will send messages on a ZeroMQ socket with a given maximum throughput.

## Installing

Add this to your ``project.clj``:

     [dispatcher "0.0.1-SNAPSHOT"]

And run ``lein install`` from the main directory to install the library.

## Usage

The usage is fairly simple, just check out the ``create``, ``enqueue``, ``expect``, ``finish`` and ``abort`` functions' documentation.
