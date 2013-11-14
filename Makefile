
compile:
	mkdir -p build/compile
	rustc --out-dir build/compile --lib src/libtrees/lib.rs

check-compile:
	mkdir -p build/
	rustc  -o ./build/libtree_check --test src/libtrees/lib.rs
check: check-compile
	./build/libtree_check $(TESTNAME)
bench:
	mkdir -p build/test
	rustc -O -o ./build/libtree_bench --test src/libtrees/lib.rs
	./build/libtree_bench --bench $(TESTNAME)



debug:
	mkdir -p build/debug
	rustc -o build/debug/libtrees_debug --test -Z debug-info src/libtrees/lib.rs
	cd src/libtrees && gdb -tui --args ./../../build/debug/libtrees_debug $(TESTNAME)

clean:
	rm -rf ./build
