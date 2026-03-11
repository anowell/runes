set dotenv-load
set positional-arguments

cli *args:
	cargo run -p runes -- {{args}}

fix:
	cargo clippy --fix
	cargo fmt

check:
    cargo check
