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
package es.bsc.compss.executor;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * Binds the calling thread to a set of CPUs. Replaces {@code es.bsc.wdc.affinity.ThreadAffinity}, whose mask is a
 * single {@code long}: CPU {@code c} lands on bit {@code c % 64}, repeated bits are added instead of OR-ed, and
 * {@code sched_setaffinity} is told the mask is 8 bytes long. On nodes with more than 64 CPUs, tasks were bound to CPUs
 * they were not assigned and shared them with other tasks.
 */
public final class CpuAffinity {

    private static final int BITS_PER_WORD = Long.SIZE;


    private interface CLibrary extends Library {

        CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);


        int sched_setaffinity(int pid, long cpusetsize, long[] mask) throws LastErrorException;
    }


    private CpuAffinity() {
        // Static methods only
    }

    /**
     * Returns the {@code cpu_set_t} words that hold the given CPUs: CPU {@code c} is bit {@code c % 64} of word
     * {@code c / 64}.
     *
     * @param cpus CPU ids, as assigned to the task.
     * @return The mask, with as many words as the highest CPU id needs.
     * @throws IllegalArgumentException If {@code cpus} is null, empty, or holds a negative id.
     */
    public static long[] mask(int[] cpus) {
        if (cpus == null || cpus.length == 0) {
            throw new IllegalArgumentException("No CPUs to set in the affinity mask");
        }
        int maxCpu = 0;
        for (int cpu : cpus) {
            if (cpu < 0) {
                throw new IllegalArgumentException("Negative CPU id in the affinity mask: " + cpu);
            }
            maxCpu = Math.max(maxCpu, cpu);
        }
        long[] words = new long[maxCpu / BITS_PER_WORD + 1];
        for (int cpu : cpus) {
            words[cpu / BITS_PER_WORD] |= 1L << (cpu % BITS_PER_WORD);
        }
        return words;
    }

    /**
     * Binds the calling thread to the given CPUs. Processes it starts afterwards inherit the binding.
     *
     * @param cpus CPU ids, as assigned to the task.
     * @throws IllegalArgumentException If {@code cpus} is null, empty, or holds a negative id.
     * @throws LastErrorException If {@code sched_setaffinity} fails.
     */
    public static void setCurrentThreadAffinity(int[] cpus) {
        setCurrentThreadAffinity(cpus,
            (pid, cpusetsize, mask) -> CLibrary.INSTANCE.sched_setaffinity(pid, cpusetsize, mask));
    }

    /**
     * Binds the calling thread to the given CPUs through {@code syscall}, which stands for {@code sched_setaffinity}.
     *
     * @param cpus CPU ids, as assigned to the task.
     * @param syscall Receives pid 0, the mask size in bytes, and the mask.
     * @throws IllegalArgumentException If {@code cpus} is null, empty, or holds a negative id.
     */
    static void setCurrentThreadAffinity(int[] cpus, SetAffinity syscall) {
        long[] words = mask(cpus);
        syscall.call(0, (long) words.length * Long.BYTES, words);
    }


    /**
     * Signature of {@code sched_setaffinity}.
     */
    interface SetAffinity {

        int call(int pid, long cpusetsize, long[] mask);
    }
}
