/*
 * Copyright 2019 Phaneesh Nagaraja <phaneesh.n@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hbase.haxwell.api;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.Cell;

import java.util.ArrayList;
import java.util.List;

public class HaxwellEvent {

    private final byte[] table;
    private final byte[] row;
    private final List<Cell> keyValues;

    public HaxwellEvent(byte[] table, byte[] row, List<Cell> keyValues) {
        this.table = table;
        this.row = row;
        this.keyValues = keyValues;
    }

    public static HaxwellEvent create(byte[] table, byte[] row, List<Cell> cells) {
        List<Cell> keyValues = new ArrayList<Cell>(cells.size());
        keyValues.addAll(cells);
        return new HaxwellEvent(table, row, keyValues);
    }

    public byte[] getTable() {
        return table;
    }

    public byte[] getRow() {
        return row;
    }

    public List<Cell> getKeyValues() {
        return keyValues;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        HaxwellEvent rhs = (HaxwellEvent) obj;
        return new EqualsBuilder().append(table, rhs.table).append(row, rhs.row).append(keyValues, rhs.keyValues).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(table).append(row).append(keyValues).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append(table).append(row)
                .append(keyValues).toString();
    }
}
