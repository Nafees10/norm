module app;
version (norm_test){
	import normtest;
	import std.stdio;

	void main(){
		stderr.writeln("running norm tests");
		test;
	}
}
