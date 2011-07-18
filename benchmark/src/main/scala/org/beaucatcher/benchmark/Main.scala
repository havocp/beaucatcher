package org.beaucatcher.benchmark

object Main {
    def main(args : Array[String]) : Unit = {
        val benchmarks = Seq(new backends.RawJavaBenchmark,
            new backends.PlainCasbahBenchmark,
            new backends.PlainHammersmithBenchmark)
        val benchmarker = new MongoBenchmarker

        benchmarker.run(benchmarks)
    }
}
