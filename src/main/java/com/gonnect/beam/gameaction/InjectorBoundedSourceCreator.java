/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.gonnect.beam.gameaction;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A {@link BoundedSource} generating a fixed number of {@link GameActionInfo}
 * events.
 */
public class InjectorBoundedSourceCreator extends BoundedSource<GameActionInfo> {

    private static final long ESTIMATED_ITEM_SIZE_BYTES = 32;

    private final InjectorIterator.SourceConfig config;

    public InjectorBoundedSourceCreator(int numEntries, int minQps, int maxQps) {
        this(new InjectorIterator.SourceConfig(numEntries, 15, minQps, maxQps));
    }

    private InjectorBoundedSourceCreator(InjectorIterator.SourceConfig config) {
        this.config = config;
    }

    @Override
    public List<? extends BoundedSource<GameActionInfo>> split(long desiredBundleSizeBytes,
                                                               PipelineOptions options) throws Exception {

        // Each source will generate all the data for a specific team. We have 15
        // numTeams, so at most
        // we can split into 15 parts.
        int desiredBundles = (int) Math.min(config.numTeams,
                (config.numEntries * ESTIMATED_ITEM_SIZE_BYTES) / desiredBundleSizeBytes);

        ArrayList<InjectorBoundedSourceCreator> shards = new ArrayList<>(desiredBundles);
        for (InjectorIterator.SourceConfig splitConfig : config.split(desiredBundles)) {
            shards.add(new InjectorBoundedSourceCreator(splitConfig));
        }
        return shards;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return config.numEntries * ESTIMATED_ITEM_SIZE_BYTES;
    }

    @Override
    public BoundedReader<GameActionInfo> createReader(PipelineOptions options) throws IOException {
        return new InjectorBoundedReader(this);
    }

    @Override
    public void validate() {
    }

    @Override
    public Coder<GameActionInfo> getDefaultOutputCoder() {
        return SerializableCoder.of(GameActionInfo.class);
    }

    private static class InjectorBoundedReader extends BoundedSource.BoundedReader<GameActionInfo> {
        private static final Logger LOG = LoggerFactory.getLogger(InjectorBoundedReader.class);
        private InjectorBoundedSourceCreator source;
        private final Iterator<GameActionInfo> items;
        private GameActionInfo current = null;

        public InjectorBoundedReader(InjectorBoundedSourceCreator source) {
            this.items = new InjectorIterator(source.config);
            this.source = source;
            LOG.error("Creating reader for numEntries={} numTeams={} minQps={} maxQps={}", source.config.numEntries,
                    source.config.numTeams, source.config.minQps, source.config.maxQps);
        }

        @Override
        public BoundedSource<GameActionInfo> getCurrentSource() {
            return source;
        }

        @Override
        public boolean start() throws IOException {
            return advance();
        }

        @Override
        public boolean advance() throws IOException {
            if (items.hasNext()) {
                current = items.next();
                return true;
            }
            return false;
        }

        @Override
        public GameActionInfo getCurrent() throws NoSuchElementException {
            return current;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return current.getTimestamp();
        }

        @Override
        public void close() throws IOException {
        }

    }

}
