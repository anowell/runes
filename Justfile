set dotenv-load
set positional-arguments

cli *args:
	cargo run -p runes -- {{args}}

check:
    cargo check
