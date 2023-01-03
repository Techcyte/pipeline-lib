#!/bin/bash

# requires graphviz to be installed,
# can be installed with `apt install graphviz` on ubuntu

dot -Tpng -o docs/producer_bound.png docs/producer_bound.dot
neato -Tpng -o docs/sequential_chain.png docs/sequential_chain.dot
