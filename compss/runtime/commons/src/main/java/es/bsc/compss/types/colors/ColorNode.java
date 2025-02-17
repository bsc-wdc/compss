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
package es.bsc.compss.types.colors;

public class ColorNode {

    private final String fillColor;
    private final String fontColor;


    /**
     * Creates a new ColorNode instance.
     * 
     * @param fillColor Fill color.
     * @param fontColor Font color.
     */
    public ColorNode(String fillColor, String fontColor) {
        this.fillColor = fillColor;
        this.fontColor = fontColor;
    }

    /**
     * Returns the fill color.
     * 
     * @return The fill color.
     */
    public String getFillColor() {
        return this.fillColor;
    }

    /**
     * Returns the font color.
     * 
     * @return The font color.
     */
    public String getFontColor() {
        return this.fontColor;
    }

}
