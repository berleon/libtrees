
compile:
	mkdir -p build/compile
	rustc --out-dir build/compile --lib src/libtrees/lib.rs

test-compile:
	mkdir -p build/test
	rustc  -o ./build/test/libtree_test --test src/libtrees/lib.rs
test: test-compile
	./build/test/libtree_test $(TESTNAME)
bench:
	mkdir -p build/test
	rustc -O -o ./build/test/libtree_bench --test src/libtrees/lib.rs
	./build/test/libtree_bench --bench $(TESTNAME)



debug:
	mkdir -p build/debug
	rustc -o build/debug/libtrees_debug --test -Z debug-info src/libtrees/lib.rs
	cd src/libtrees && gdb -tui --args ./../../build/debug/libtrees_debug $(TESTNAME)

clean:
	rm -rf ./build
