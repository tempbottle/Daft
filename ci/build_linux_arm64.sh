#!/bin/sh

curl https://sh.rustup.rs -sSf | sh -s -- --profile minimal --default-toolchain nightly
pip3 install "maturin[readelf]"
ls
