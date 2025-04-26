#!/bin/sh

export ENV_KEYS_R=service.name,process.id,service.version
export ENV_KEYS_A=http.status,url.path,user.id,order.id,flag.experimental

input="./sample.d/input.jsonl"
output="./sample.d/output.asn1.multi.gz"

geninput(){
	echo generating input data...
	mkdir -p sample.d

	jq -c -n '[
		{
			time: "2025-04-10T02:44:55.012345Z",
			severity:"trace",
			body:"helo",
			"service.name": "test service",
			"http.status": 200,
			"url.path": "/path/to/api/v1",
			"user.id": "jd",
			"flag.experimental": false,
			"order.id": 299792458
		},
		{time: "2025-04-11T02:44:55.012345Z", severity:"debug", body:"helo"},
		{time: "2025-04-12T02:44:55.012345Z", severity:"info", body:"helo"},
		{time: "2025-04-13T02:44:55.012345Z", severity:"warn", body:"helo"},
		{time: "2025-04-14T02:44:56.012345Z", severity:"error", body:"wrld"},
		{time: "2025-04-15T02:44:56.012345Z", severity:"fatal", body:"wrld"}
	]' |
		jq -c '.[]' |
		cat > "${input}"
	
}

test -f "${input}" || geninput

echo
echo creating multi stream gzip file...
cat "${input}" |
	./jsonlogs2asn1gzip |
	cat > "${output}"

ls -lSh \
	"${input}" \
	"${output}"

which dasel | fgrep -q dasel || exec sh -c 'echo dasel missing.; exit 1'
which bat | fgrep -q bat || exec sh -c 'echo bat missing.; exit 1'

cat "${output}" |
	zcat |
	tail --bytes=+173 |
	head --bytes=36 |
	xxd -ps |
	tr -d '\n' |
	python3 \
		-m asn1tools \
		convert \
		-i der \
		-o jer \
		./flatlog.asn \
		LogItem \
		- |
	dasel \
		--read=json \
		--write=yaml |
	bat --language=yaml

which fq | fgrep -q fq || exec sh -c 'echo fq missing.; exit 1'

cat "${output}" |
	fq \
		--decode gzip \
		'.members'
