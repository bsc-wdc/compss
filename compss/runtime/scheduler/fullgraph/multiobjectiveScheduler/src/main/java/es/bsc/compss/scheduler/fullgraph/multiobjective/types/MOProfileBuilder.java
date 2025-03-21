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
package es.bsc.compss.scheduler.fullgraph.multiobjective.types;

import es.bsc.compss.scheduler.types.ProfileBuilder;


public class MOProfileBuilder extends ProfileBuilder {

    private double power = MOProfile.DEFAULT_POWER;
    private double price = MOProfile.DEFAULT_PRICE;


    /**
     * Creates new MO Profile Builder.
     */
    public MOProfileBuilder() {
        super();
    }

    /**
     * Builds the MO Profile.
     */
    public MOProfile build() {
        MOProfile p = new MOProfile();
        update(p);
        return p;
    }

    /**
     * Sets a new power.
     * 
     * @param power New power.
     */
    public void setPower(double power) {
        this.power = power;
    }

    /**
     * Sets a new price.
     * 
     * @param price New price.
     */
    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * Updates the current profile builder with the given profile information.
     * 
     * @param p Profile to update with.
     */
    protected <P extends MOProfile> void update(P p) {
        super.update(p);
        MOProfile prof = (MOProfile) p;
        prof.setPower(this.power);
        prof.setPrice(this.price);
    }
}
