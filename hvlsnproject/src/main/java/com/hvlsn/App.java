/**
 * Furkan Celen
 * Made for Havelsan Kestirim Project
 * */

package com.hvlsn;

import java.lang.Math;
import java.util.Random;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class App{
    private static final String servtopic="ftopic";
    private static final String boot_server="localhost:9092";
    
    /*
    Main method for starting threads
    */
    public static void main(String[] args) throws Exception {
        System.out.printf("Starting program..\n");

        // starting threads to communications
        Thread consumerThread = new Thread(App::consumer);
        consumerThread.start();

        Thread producerThread = new Thread(App::producer);
        producerThread.start();
            
    }

    /*
    Kafka producer method , it works as our sensors in project
    */
    private static void producer() {


        // for coordinate system limitations of our map 1000x1000 
        int min = -500,max = 500;  

        //Generate random int in range  
        Random random = new Random();
         
        int fsensor_x = random.nextInt(max - min) + min; // sensor's x coordinate
        int fsensor_y = random.nextInt(max - min) + min; // sensor's y coordinate
        int fsensor_t = 68; // sensor's estimation direction degree to target
        
        
        
        // second sensor
        int ssensor_x = random.nextInt(max - min) + min; 
        int ssensor_y = random.nextInt(max - min) + min;
        int ssensor_t = 423; // sensor's estimation direction degree to target

        // Manually set for testing purposes , can be comment with // for random locations
        fsensor_x=-5;fsensor_y=1;fsensor_t=45;
        ssensor_x=5;ssensor_y=-1;ssensor_t=315;


        System.out.println("Coordinates of sensors s1="+fsensor_x+","+fsensor_y+"  s2="+ssensor_x+","+ssensor_y);

        // Configurations to create new producer
        Properties props=new Properties();
        props.put("bootstrap.servers", boot_server);
        
        //serializers for message info
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    
        try (Producer<Integer, Integer> myproducer = new KafkaProducer<>(props)) {
            
            // 6 times message for sensor location and target direction infos for both
            for (int i=0;i<6; i++) {
                int key=1 , message=0;
                
                switch(i) {
                  case 0:
                    message=fsensor_x;
                    break;
                  case 1:
                    message=fsensor_y;
                    break;
                  case 2:
                    message=fsensor_t;
                    break;
                  case 3:
                    message=ssensor_x;
                    break;
                  case 4:
                    message=ssensor_y;
                    break;
                  case 5:
                    message=ssensor_t;
                    break;
                            
                  default:
                    message=0;
                }
                

                myproducer.send(new ProducerRecord<Integer, Integer>(servtopic, key, message));

                // log a confirmation once the message is written
                System.out.println("sent::" + message);
                try {
                    // Sleep for a second
                    Thread.sleep(1000);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Facing error while creating producer " + e);
        }
        System.out.println("Sensors all info sent");
    }


    /*
    Kafka consumer method , it works as our central unit in project
    */
    private static void consumer() {
            // Configurations to create new consumer

            int fsensor_x=0 ,fsensor_y=0,fsensor_t=0,ssensor_x=0,ssensor_y=0 ,ssensor_t=0 , temp=0; 


            Properties props = new Properties();
            props.setProperty("bootstrap.servers", boot_server);
            props.setProperty("group.id", "my-group-id");
            
            // deserializer configuration used Integer
            //props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            //props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
            props.setProperty("value.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");


            // to saving messages we enable the auto commit feature
            props.setProperty("enable.auto.commit", "true");
            props.setProperty("auto.commit.interval.ms", "1000");

            
            try (KafkaConsumer<Integer, Integer> myconsumer = new KafkaConsumer<>(props)) {
                // join the server topic
                myconsumer.subscribe(Arrays.asList(servtopic));
               
                int count=0;
                // loop until all 6 message received
                while (count<6) {

                    // consumer waits for new messages with poll method
                    ConsumerRecords<Integer, Integer> record_buffer = myconsumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<Integer, Integer> record : record_buffer){
                    	
                        System.out.printf("received message:%d\n", record.value());
                        
                        switch(count) {
                          case 0:
                            fsensor_x=record.value();
                            break;
                          case 1:
                            fsensor_y=record.value();
                            break;
                          case 2:
                            fsensor_t=record.value();
                            break;
                          case 3:
                            ssensor_x=record.value();
                            break;
                          case 4:
                            ssensor_y=record.value();
                            break;
                          case 5:
                            ssensor_t=record.value();
                            break;
                                    
                          default:
                            temp=record.value();
                        }
                        count++;
                    }
                    
                    
                }
            }
            
            System.out.println("All infos received , calculating target's location:\n fsensor_x="+fsensor_x+":fsensor_y="+fsensor_y);
            System.out.println("ssensor_x="+ssensor_x+":ssensor_y="+ssensor_y);
            System.out.println("fsensor_t="+fsensor_t+":ssensor_t="+ssensor_t);

            //calculate slop of equation from received estimation values of sensors  
            double fdb = Math.toRadians(fsensor_t);
            fdb = Math.tan(fdb); // find slop from tanjent value
            double fslop=fdb;

            double sdb = Math.toRadians(ssensor_t);
            sdb = Math.tan(sdb); // find slop from tanjent value
            double sslop=sdb;

            System.out.printf("first's slop=%.2f second's slop=%.2f",fslop,+sslop);
            
            if(fslop == sslop) {
            	System.out.println("Slopes of the lines are same , there is no cross \n Can't find result!!");
            	return;
            }
            //calculate coefficients of equations for find target location method
            double fa=-fslop , fb=1 , fe=fsensor_y-(fslop*fsensor_x);
            double sc=-sslop , sd=1 , sf=ssensor_y-(sslop*ssensor_x);

            findTargetLocation(fa,fb,sc,sd,fe,sf);
            	
    }

    //solves two equations with parameters , finds location
    // ax+by=e  , cx+dy=f   : two equations coefficient names used below
    public static void findTargetLocation(double a, double b, double c, double d, double e, double f) {

        double ind = ((a) * (d) - (b) * (c));
        double x = ((d) * (e) - (b) * (f)) / ind;
        double y = ((a) * (f) - (c) * (e)) / ind;
        System.out.printf("\nLocation of the Target (%.2f,%.2f)\n" , x , y);
    

    }
    


}//end of App Class