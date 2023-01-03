#!/bin/bash

# requires graphviz to be installed,
# can be installed with `apt install graphviz` on ubuntu

dot -Tpng -o docs/producer_bound.png docs/producer_bound.dot
dot -Tpng -o docs/consumer_bound.png docs/consumer_bound.dot
dot -Tpng -o docs/sequential_chain.png docs/sequential_chain.dot
