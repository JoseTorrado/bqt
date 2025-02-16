.PHONY: run
run:
	@go run cmd/bqt/bqt.go t --tests tests_data 2>&1 | grep -Ev "In file included from|note: expanded from|<scratch space>:|bind\.cc:|FLAGS_zetasql|/Library/Developer/CommandLineTools/SDKs/MacOSX\.sdk/|expanded from macro|go-zetasql"
