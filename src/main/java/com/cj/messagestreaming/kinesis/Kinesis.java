package com.cj.messagestreaming.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.cj.messagestreaming.Types;

import java.util.stream.Stream;

public class Kinesis{

    public static <T> void publish(KinesisConfig config, Stream<T> data) {

    }


    private static Stream<byte[]> subscribe(KinesisConfig config){
        IRecordProcessor processor = new IRecordProcessor(){

            @Override
            public void initialize(InitializationInput initializationInput) {

            }

            @Override
            public void processRecords(ProcessRecordsInput processRecordsInput) {

            }

            @Override
            public void shutdown(ShutdownInput shutdownInput) {

            }
        };

        IRecordProcessorFactory factory = new IRecordProcessorFactory() {
            @Override
            public IRecordProcessor createProcessor() {
                return processor;
            }
        };

        Worker worker = new Worker.Builder().recordProcessorFactory(factory).build();
        
        //Should we spawn a thread here?
        return null;
    }
}
