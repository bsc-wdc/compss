/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.types;

import java.util.HashSet;
import java.util.Set;


public class ActionGroup {

    private final Set<AllocatableAction> actions;


    public ActionGroup() {
        actions = new HashSet<>();
    }

    /**
     * Adds a new action to the group.
     *
     * @param newMember new action added to the group
     */
    public final void addMember(AllocatableAction newMember) {
        actions.add(newMember);
    }

    /**
     * Removes an action from the group.
     *
     * @param member action to be removed
     */
    public final void removeMember(AllocatableAction member) {
        actions.remove(member);
    }

    /**
     * Returns all the members of the group.
     *
     * @return members of the group
     */
    public final Iterable<AllocatableAction> getMembers() {
        return actions;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ActionGroup@").append(this.hashCode());
        for (AllocatableAction action : getMembers()) {
            sb.append(action.toString());
        }
        return sb.toString();
    }


    public static class MutexGroup extends ActionGroup {

        private AllocatableAction lockHolder;


        public MutexGroup() {
            this.lockHolder = null;
        }

        /**
         * Returns whether a given action can start executing or not.
         *
         * @param action Action to ask if it is being executed.
         * @return {@literal true} if the given action could start, {@literal false} otherwise.
         */
        public boolean testLock(AllocatableAction action) {
            return this.lockHolder == null || this.lockHolder == action;
        }

        /**
         * Group starts processing execution.
         *
         * @param action action acquiring the lock
         */
        public void acquireLock(AllocatableAction action) {
            this.lockHolder = action;
        }

        /**
         * The group ends processing execution.
         */
        public void releaseLock() {
            lockHolder = null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("MutexGroup@").append(this.hashCode()).append("[");
            for (AllocatableAction action : getMembers()) {
                sb.append(" ").append((action == lockHolder) ? "-" + action.toString() + "-" : action.toString());
            }
            sb.append("]");
            return sb.toString();
        }
    }
}
