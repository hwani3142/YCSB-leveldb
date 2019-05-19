/*
 * Copyright 2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.List;
import java.nio.ByteBuffer;
import com.yahoo.mapkeeper.*;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;

class StubServer implements MapKeeper.Iface {
    public ResponseCode ping() throws TException
    {
        return ResponseCode.Success;
    }

    public ResponseCode addMap(String databaseName) throws TException
    {
        return ResponseCode.Success;
    }

    public ResponseCode dropMap(String databaseName) throws TException
    {
        return ResponseCode.Success;
    }

    public StringListResponse listMaps() throws TException
    {
        StringListResponse response = new StringListResponse();
        response.responseCode = ResponseCode.Success;
        return response;
    }

    public RecordListResponse scan(String databaseName, ScanOrder order, 
        ByteBuffer startKey, boolean startKeyIncluded, 
        ByteBuffer endKey, boolean endKeyIncluded, 
        int maxRecords, int maxBytes) throws TException 
    {
        RecordListResponse response = new RecordListResponse();
        response.responseCode = ResponseCode.Success;
        return response;
    }

    public BinaryResponse get(String databaseName, ByteBuffer recordKey) throws TException
    {
        BinaryResponse response = new BinaryResponse();
        response.responseCode = ResponseCode.Success;
        return response;
    }

    public ResponseCode put(String databaseName, ByteBuffer recordKey, ByteBuffer recordValue) throws TException
    {
        return ResponseCode.Success;
    }

    public ResponseCode insert(String databaseName, ByteBuffer recordKey, ByteBuffer recordValue) throws TException
    {
        return ResponseCode.Success;
    }

    public ResponseCode insertMany(String databaseName, List<Record> records) throws TException {
        return ResponseCode.Success;
    }
    
    public ResponseCode update(String databaseName, ByteBuffer recordKey, ByteBuffer recordValue) throws TException
    {
        return ResponseCode.Success;
    }

    public ResponseCode remove(String databaseName, ByteBuffer recordKey) throws TException
    {
        return ResponseCode.Success;
    }

    public static void usage() {
        System.err.println("Usage: java -jar stub_server.jar [hsha|nonblocking|threadpool|selector]");
        System.exit(1);
    }

    public static void main(String argv[]) {
        try {
            int port = 9090;
            int numThreads = 32;
            if (argv.length != 1) {
                usage();
            }
            System.out.println(argv[0]);
            StubServer mapkeeper = new StubServer();
            TServer server = null;
            if (argv[0].equals("hsha")) {
                TNonblockingServerTransport trans = new TNonblockingServerSocket(port);
                THsHaServer.Args args = new THsHaServer.Args(trans);
                args.transportFactory(new TFramedTransport.Factory());
                args.protocolFactory(new TBinaryProtocol.Factory());
                args.processor(new MapKeeper.Processor(mapkeeper));
                args.workerThreads(numThreads);
                server = new THsHaServer(args);
            } else if (argv[0].equals("nonblocking")) {
                TNonblockingServerTransport trans = new TNonblockingServerSocket(port);
                TNonblockingServer.Args args = new TNonblockingServer.Args(trans);
                args.transportFactory(new TFramedTransport.Factory());
                args.protocolFactory(new TBinaryProtocol.Factory());
                args.processor(new MapKeeper.Processor(mapkeeper));
                server = new TNonblockingServer(args);
            } else if (argv[0].equals("threadpool")) {
                TServerTransport trans = new TServerSocket(port);
                TThreadPoolServer.Args args = new TThreadPoolServer.Args(trans);
                args.transportFactory(new TFramedTransport.Factory());
                args.protocolFactory(new TBinaryProtocol.Factory());
                args.processor(new MapKeeper.Processor(mapkeeper));
                server = new TThreadPoolServer(args);
            } else if (argv[0].equals("selector")) {
                TNonblockingServerTransport trans = new TNonblockingServerSocket(port);
                TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(trans);
                args.transportFactory(new TFramedTransport.Factory());
                args.protocolFactory(new TBinaryProtocol.Factory());
                args.processor(new MapKeeper.Processor(mapkeeper));
                args.selectorThreads(4);
                args.workerThreads(numThreads);
                server = new TThreadedSelectorServer(args);
            } else {
                usage();
            }
            server.serve();
        } catch (Exception x) {
            System.out.println(x.toString() + " " + x.getMessage());
        }
    }
}
