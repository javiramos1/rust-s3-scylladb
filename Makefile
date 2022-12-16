TAG = 0.0.1

db: 
	docker-compose up
stop:
	docker-compose down

bld:
	cargo build
run:
	cargo run

build:
	DOCKER_BUILDKIT=1 docker build -t rust-s3-scylladb/rust-s3-scylladb-svc:$(TAG) .

buildx:
	docker buildx build --load --platform=linux/arm64 -t rust-s3-scylladb/rust-s3-scylladb-svc:$(TAG) .

push:
	docker push rust-s3-scylladb/rust-s3-scylladb-svc:$(TAG)
