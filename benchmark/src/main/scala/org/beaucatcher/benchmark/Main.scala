package org.beaucatcher.benchmark

object Main {
    def main(args : Array[String]) : Unit = {
        val include =
            if (args.length > 1) {
                for (i <- 1 until args.length)
                    yield args(i).toLowerCase.replace(" ", "")
            } else {
                Seq()
            }

        def includeBenchmark(b : MongoBenchmark[_]) = {
            if (include.length == 0) {
                true
            } else {
                include.contains(b.name.toLowerCase.replace(" ", ""))
            }
        }

        val benchmarks = Seq(new backends.RawJavaBenchmark,
            new backends.PlainCasbahBenchmark,
            new backends.PlainHammersmithBenchmark) filter includeBenchmark

        if (benchmarks.isEmpty) {
            System.err.println("Unknown benchmark names")
            System.exit(1)
        } else {
            printf("Enabling benchmarks: %s\n", benchmarks map { _.name } mkString ("", ",", ""))
        }

        val benchmarker = new MongoBenchmarker

        benchmarker.run(benchmarks)
    }
}
