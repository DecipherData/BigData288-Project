/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Flinkwebsiteanalysis;

import Deserializer.JSONValueDeserializationSchema;
import Dto.UserSession;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.ActionListener;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.DocWriteRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexResponse;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RequestOptions;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestHighLevelClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static utils.JsonUtil.convertSessionToJson;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = "user_session_info";

		KafkaSource<UserSession> source = KafkaSource.<UserSession>builder()
				.setBootstrapServers("kafka:29092")
				.setTopics(topic)
				.setGroupId("Flink-Group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();

		DataStream<UserSession> sessionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),  "Kafka Source");

//		sessionStream.print();
		UserSessionEmitter emitter = new UserSessionEmitter();
		sessionStream.sinkTo(
				new Elasticsearch7SinkBuilder<UserSession>()
						.setHosts(new HttpHost("10.0.0.244", 9200, "http"))
						.setEmitter(emitter)
						.setBulkFlushMaxActions(5)
//						.setBulkFlushInterval(1000L)
						.setConnectionTimeout(10)
						//.setBulkFlushMaxSizeMb(1)
						.build()
		).name("Elasticsearch Sink");

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}

class UserSessionEmitter implements ElasticsearchEmitter<UserSession>{

	static int count = 6;
	@Override
	public void open() {
		System.out.println("open " + count);
	}

	@Override
	public void emit(UserSession session, SinkWriter.Context context, RequestIndexer requestIndexer) {
		try {
			String json = convertSessionToJson(session);

			System.out.println("Adding session  - > " + count + json);

			IndexRequest indexRequest = Requests.indexRequest()
					.index("sessionusers")
					.id(session.getSessionId())
					.source(json, XContentType.JSON);

			getClient().indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
				@Override
				public void onResponse(IndexResponse indexResponse) {
				}

				@Override
				public void onFailure(Exception e) {
					System.err.println("Error during indexing: " + e.getMessage());
				}
			});

//				requestIndexer.add(indexRequest);
		}catch (Throwable ex) {
			System.err.println("Error while emitting to Elasticsearch: " + ex.getMessage());
			ex.printStackTrace();
		}
	}

	@Override
	public void close() {
		System.out.println("close");
	}

	private static RestHighLevelClient client = null;
	public static RestHighLevelClient getClient() {
		if (client == null) {
			client = new RestHighLevelClient(
					RestClient.builder(new HttpHost("10.0.0.244" ,  9200, "http")));
		}
		return client;
	}
}

