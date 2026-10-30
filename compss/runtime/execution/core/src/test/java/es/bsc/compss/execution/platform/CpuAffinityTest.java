/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.execution.platform;

import com.sun.jna.LastErrorException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class CpuAffinityTest {

    private static final Path THREAD_STATUS = Paths.get("/proc/thread-self/status");


    private static long bits(int from, int to) {
        long word = 0L;
        for (int bit = from; bit <= to; ++bit) {
            word |= 1L << bit;
        }
        return word;
    }

    @Test
    public void cpusBelow64StayInFirstWord() {
        Assert.assertArrayEquals(new long[] { bits(56, 62) },
            CpuAffinity.mask(IntStream.rangeClosed(56, 62).toArray()));
    }

    @Test
    public void cpu63SetsTheSignBit() {
        Assert.assertArrayEquals(new long[] { Long.MIN_VALUE }, CpuAffinity.mask(new int[] { 63 }));
    }

    @Test
    public void cpusAbove63GoToTheirOwnWord() {
        // ThreadAffinity 0.0.2-3 bound CPUs 77-83 to CPUs 13-19 on MareNostrum 5
        long[] mask = CpuAffinity.mask(IntStream.rangeClosed(77, 83).toArray());

        Assert.assertArrayEquals(new long[] { 0L,
            bits(13, 19) }, mask);
    }

    @Test
    public void wholeSocketWithHyperthreadsOn224CpuNode() {
        // ThreadAffinity 0.0.2-3 bound these 112 CPUs to 1-31,40-47,56-63 on MareNostrum 5
        int[] cpus = IntStream.concat(IntStream.rangeClosed(56, 111), IntStream.rangeClosed(168, 223)).toArray();

        long[] mask = CpuAffinity.mask(cpus);

        Assert.assertArrayEquals(new long[] { bits(56, 63),
            bits(0, 47),
            bits(40, 63),
            bits(0, 31) }, mask);
    }

    @Test
    public void repeatedCpuIsSetOnce() {
        Assert.assertArrayEquals(new long[] { 1L << 5 }, CpuAffinity.mask(new int[] { 5,
            5 }));
    }

    @Test
    public void wordBoundaries() {
        Assert.assertArrayEquals(new long[] { 1L }, CpuAffinity.mask(new int[] { 0 }));
        Assert.assertArrayEquals(new long[] { 0L,
            1L }, CpuAffinity.mask(new int[] { 64 }));
        Assert.assertArrayEquals(new long[] { Long.MIN_VALUE,
            1L }, CpuAffinity.mask(
                new int[] { 63,
                    64 }));
        Assert.assertArrayEquals(new long[] { 0L,
            Long.MIN_VALUE }, CpuAffinity.mask(new int[] { 127 }));
        Assert.assertArrayEquals(new long[] { 0L,
            0L,
            1L }, CpuAffinity.mask(new int[] { 128 }));
    }

    @Test
    public void unsortedAndRepeatedCpusGiveTheSameMask() {
        Assert.assertArrayEquals(CpuAffinity.mask(new int[] { 5,
            70 }), CpuAffinity.mask(
                new int[] { 70,
                    5,
                    70,
                    5 }));
    }

    @Test
    public void syscallGetsTheWholeMaskOn224CpuNode() {
        // ThreadAffinity 0.0.2-3 always passed 8 bytes, so the kernel never saw CPUs 64-223
        int[] cpus = IntStream.concat(IntStream.rangeClosed(56, 111), IntStream.rangeClosed(168, 223)).toArray();
        AtomicReference<Long> size = new AtomicReference<>();
        AtomicReference<long[]> words = new AtomicReference<>();

        CpuAffinity.setCurrentThreadAffinity(cpus, (pid, cpusetsize, mask) -> {
            Assert.assertEquals(0, pid);
            size.set(cpusetsize);
            words.set(mask.clone());
            return 0;
        });

        Assert.assertEquals(Long.valueOf(4 * Long.BYTES), size.get());
        Assert.assertArrayEquals(new long[] { bits(56, 63),
            bits(0, 47),
            bits(40, 63),
            bits(0, 31) }, words.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullCpusAreRejected() {
        CpuAffinity.mask(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyCpusAreRejected() {
        CpuAffinity.mask(new int[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeCpuIsRejected() {
        CpuAffinity.mask(new int[] { 3,
            -1 });
    }

    @Test
    public void bindsCallingThreadToAllowedCpu() throws Exception {
        Assume.assumeTrue("Needs Linux /proc/thread-self", Files.isReadable(THREAD_STATUS));

        AtomicReference<String> before = new AtomicReference<>();
        AtomicReference<String> after = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<LastErrorException> denied = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                before.set(allowedCpus());
                int cpu = Integer.parseInt(before.get().split("[-,]")[0]);
                CpuAffinity.setCurrentThreadAffinity(new int[] { cpu });
                after.set(allowedCpus());
            } catch (LastErrorException e) {
                denied.set(e);
            } catch (Throwable t) {
                error.set(t);
            }
        });
        thread.start();
        thread.join();

        Assume.assumeTrue("sched_setaffinity not permitted here: " + denied.get(), denied.get() == null);
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        Assert.assertEquals(before.get().split("[-,]")[0], after.get());
    }

    private static String allowedCpus() throws IOException {
        for (String line : Files.readAllLines(THREAD_STATUS)) {
            if (line.startsWith("Cpus_allowed_list:")) {
                return line.substring("Cpus_allowed_list:".length()).trim();
            }
        }
        throw new IOException("No Cpus_allowed_list in " + THREAD_STATUS);
    }
}
