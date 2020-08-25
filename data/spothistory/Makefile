
clean:
	rm -f spotprices-us-east-1.csv

spotprices-us-east-1.csv:
	echo '"AvailabilityZone","InstanceType","ProductDescription","SpotPrice","Timestamp"' > spotprices-us-east-1.csv
	# jq -r '.SpotPriceHistory[0] | keys | @csv' >> spotprices-us-east-1.csv
	find -path \*/us-east-1/\* | xargs cat | jq -r '.SpotPriceHistory[] | map(values) | @csv' | sort -u >> spotprices-us-east-1.csv

fetch-interruptions:
	mkdir -p interruption-frequencies
	wget https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json -O interruption-frequencies/spot-data-$(shell date -I"seconds").json

all: clean spotprices-us-east-1.csv

.PHONY: clean all fetch-interruptions
