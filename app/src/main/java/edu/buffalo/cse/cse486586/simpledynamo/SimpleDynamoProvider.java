package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static String [] remotePort = {"11108","11112","11116","11120","11124"};
	private Node myNode;
	static Set<String> portSet=new HashSet<String>(Arrays.asList(remotePort));
	private String leaderPort=remotePort[0];
	//keep track of all nodes in ring.We used linked list to form ring so it is not good idea to traverse linked list
	//to just find every time if node is present in ring or not
	private Map<String,String> allNodesInRing=new TreeMap<String,String>();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		/*String requestor=myNode.getPort();
		if(selectionArgs!=null){
			if(requestor.equals(selectionArgs[0]))
				return 0;
			requestor=selectionArgs[0];
		}*/
		//delete from local all
		if(selection.equals("@")){
			deleteAllLocalFiles();
			return 0;
		}
		//delete from  all avds
		else if(selection.equals("*")){
			//only one node in ring
			/*if(myNode.getId().compareTo(myNode.getPredecessorHash())==0){
				deleteAllLocalFiles();
				return 0;
			}
			else{
				deleteAllLocalFiles();
				forwardDeleteRequest("*",myNode.getSuccessor(),requestor);
			}*/
            for(String port:allNodesInRing.values()){
                if(port.equals(myNode.getPort()))
                    deleteAllLocalFiles();
                else
                    forwardDeleteRequest("@",port);
            }
		}
		else{
			if(selection!=null) {
				try {
					//check whether current node is right node for insert else forward request to its successor
					//check whether we should insert here
					if(selectionArgs!=null){
						getContext().getApplicationContext().deleteFile(selection);
					}
					else{
						String port=getKeyBelogingPort(selection);
						String [] sucessors=getTwoSuccessors(port);
						if(port.equals(myNode.getPort())){
							getContext().getApplicationContext().deleteFile(selection);
							forwardDeleteRequest(selection,sucessors[0]);
							forwardDeleteRequest(selection,sucessors[1]);
							Log.v("deleted key at", myNode.getPort());
						}
						//forward request to successor of current node
						else{
							forwardDeleteRequest(selection,port);
							forwardDeleteRequest(selection,sucessors[0]);
							forwardDeleteRequest(selection,sucessors[1]);
						}
					}
				}
				catch (Exception e){
					e.printStackTrace();
					Log.v("unable to delete ", "key "+selection+" not found here");
				}
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.v("details", myNode.toString());
		if(values!=null) {
			FileOutputStream outputStream=null;
			try {
				//check whether current node is right node for insert else forward request to its successor
				String key= (String) values.get(KEY_FIELD);
				String value= (String) values.get(VALUE_FIELD);
				//check whether we should insert here
                String rightPort=getKeyBelogingPort(key);
				if(rightPort.equals(myNode.getPort())){
					outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
					outputStream.write(value.getBytes());
                    String [] successors=getTwoSuccessors(myNode.getPort());
                    forwardInsertRequest(key,value,successors[0]);
                    forwardInsertRequest(key,value,successors[1]);
					Log.v("inserted key at", values.toString());
				}
				//forward request to successor of current node
				else{
					forwardInsertRequest(key,value,rightPort);
                    String [] successors=getTwoSuccessors(rightPort);
                    forwardInsertRequest(key,value,successors[0]);
                    forwardInsertRequest(key,value,successors[1]);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				Log.v("file not found", values.getAsString("key")+"\t"+values.toString());
			}
			catch (IOException e){
				e.printStackTrace();
				Log.v("insert failed", values.getAsString("key")+"\t"+values.toString());
			}

			try {
				if(outputStream!=null){
					outputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
				Log.v("Connection failed", "issue while closing connection");
			}
		}
		return uri;
	}

	private String [] getTwoSuccessors(String port){
        String arr[]= new String[allNodesInRing.size()];
        int index=0;
        for(String s:allNodesInRing.values()){
            arr[index++]=s;
        }
        int length=arr.length;
        String [] ret=new String[2];
        for(int i=0;i<arr.length;i++){
            if(arr[i].equals(port)){
                ret[0]=arr[((i+1)%length)];
                ret[1]=arr[((i+2)%length)];
            }
        }
        return ret;
    }
	private void forwardInsertRequest(String key, String value, String rightNode) {
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT",rightNode,key,value);
	}

	private void forwardDeleteRequest(String key,String rightNode) {
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETE",rightNode,key);
	}

	/*private boolean isRightNode(String key)  {
		String keyHash=getNodeHash(key);
		//for only following conditions we allow insert to current node
		//1.only one node in ring
		//2.keyHash >predecessor hash and keyHash<=current node hash and predecessor<current node hash
		//3.keyHash>=current node hash and KeyHash>PredecessorHash and PredecessorHash>current node hash
		//4.keyHash<=current node hash and keyHash<Predecessor hash and predecessor hash>current node hash

		String nodeHash=myNode.getId();
		String predecessorHash=myNode.getPredecessorHash();
		//Log.e(TAG, "test values "+key+" "+nodeHash+" "+predecessorHash);
//        if((nodeHash.compareTo(predecessorHash)==0)||
//                (keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)>0&&nodeHash.compareTo(predecessorHash)>0)||
//                (keyHash.compareTo(nodeHash)>=0&&keyHash.compareTo(predecessorHash)>0&&predecessorHash.compareTo(nodeHash)>0)||
//                (keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)<0&&predecessorHash.compareTo(nodeHash)>0)) {
		if((nodeHash.compareTo(predecessorHash)==0))
			return true;
		else if(keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)>0&&nodeHash.compareTo(predecessorHash)>0)
			return true;
		else if(keyHash.compareTo(nodeHash)>=0&&keyHash.compareTo(predecessorHash)>0&&predecessorHash.compareTo(nodeHash)>0)
			return true;
		else if(keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)<0&&predecessorHash.compareTo(nodeHash)>0)
			return true;
		else
			return false;
	}*/

    private boolean isRightNode(String keyHash,String nodeHash,String predecessorHash)  {
        //for only following conditions we allow insert to current node
        //1.only one node in ring
        //2.keyHash >predecessor hash and keyHash<=current node hash and predecessor<current node hash
        //3.keyHash>=current node hash and KeyHash>PredecessorHash and PredecessorHash>current node hash
        //4.keyHash<=current node hash and keyHash<Predecessor hash and predecessor hash>current node hash

        //Log.e(TAG, "test values "+key+" "+nodeHash+" "+predecessorHash);
//        if((nodeHash.compareTo(predecessorHash)==0)||
//                (keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)>0&&nodeHash.compareTo(predecessorHash)>0)||
//                (keyHash.compareTo(nodeHash)>=0&&keyHash.compareTo(predecessorHash)>0&&predecessorHash.compareTo(nodeHash)>0)||
//                (keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)<0&&predecessorHash.compareTo(nodeHash)>0)) {
        if((nodeHash.compareTo(predecessorHash)==0))
            return true;
        else if(keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)>0&&nodeHash.compareTo(predecessorHash)>0)
            return true;
        else if(keyHash.compareTo(nodeHash)>=0&&keyHash.compareTo(predecessorHash)>0&&predecessorHash.compareTo(nodeHash)>0)
            return true;
        else if(keyHash.compareTo(nodeHash)<=0&&keyHash.compareTo(predecessorHash)<0&&predecessorHash.compareTo(nodeHash)>0)
            return true;
        else
            return false;
    }

	private String getKeyBelogingPort(String key){
        String keyHash=getNodeHash(key);
        String arr[]= new String[allNodesInRing.size()];
        int index=0;
        for(String s:allNodesInRing.values()){
            arr[index++]=s;
        }
        Log.e(TAG,"current allNodeInRing "+allNodesInRing.size());
        int length=arr.length;
        for(int i=1;i<arr.length;i++){
            if(isRightNode(keyHash,getNodeHash(arr[i%length]),getNodeHash(arr[(i-1)%length]))){
                return arr[i%length];
            }
        }
        return arr[0];
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		//setting node itself as its predecessor and successor
		myNode=new Node(myPort,myPort,myPort);

		try {
			/*
			 * Create a server socket as well as a thread (AsyncTask) that listens on the server
			 * port.
			 *
			 * AsyncTask is a simplified thread construct that Android provides. Please make sure
			 * you know how it works by reading
			 * http://developer.android.com/reference/android/os/AsyncTask.html
			 */
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			//we assumed that port 5504(avd0) will be the leader always if it comes online
			// therefore no need to send msg to anyone
            /*if(!myPort.equals(leaderPort)){
                //telling everyone hey, i am joining the network
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JOIN", myPort);
            }
            //if current port is not avd0 then we have to every other nodes in network i am joining the network
            else{
                //add leader node to ring
                allNodesInRing.put(getNodeHash(leaderPort),leaderPort);
            }*/

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JOIN", myPort);
//            makeRing();
		} catch (IOException e) {
			/*
			 * Log is a good way to debug your code. LogCat prints out all the messages that
			 * Log class writes.
			 *
			 * Please read http://developer.android.com/tools/debugging/debugging-projects.html
			 * and http://developer.android.com/tools/debugging/debugging-log.html
			 * for more information on debugging.
			 */
			Log.e(TAG, "Can't create a ServerSocket");
			return true;
		}
		return true;
	}

	private MatrixCursor getKeysData(){
			// TODO Auto-generated method stub
			MatrixCursor cursor=new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
			String text="";
			String [] fileList=getContext().getApplicationContext().fileList();
//			for(String s:fileList)
//				Log.v("vishal files ", s);
			for(String selection:fileList) {
				try {
					// BufferedReader br=new BufferedReader(new FileReader(selection));
					FileInputStream fis = getContext().getApplicationContext().openFileInput(selection);
					text="";
					int value = 0;
					while ((value = fis.read()) != -1) {
						text += (char) value;
					}
					if (text.equals("")) {
						Log.v("file", "file not found");
						//return null;
					} else {
						cursor.addRow(new String[]{selection, text});
					}
				} catch (Exception e) {
					e.printStackTrace();
					Log.v("file", "file not found");
				}
			}
			Log.v("query", "successfully found rows "+cursor.getCount());
			return cursor;
	}

	private MatrixCursor getKey(String selection){
		{
			// TODO Auto-generated method stub
			MatrixCursor cursor=null;
			String text="";
			try{
				// BufferedReader br=new BufferedReader(new FileReader(selection));
				FileInputStream fis=getContext().getApplicationContext().openFileInput(selection);

				int value=0;
				while((value=fis.read())!=-1){
					text+=(char)value;
				}
				if(text.equals("")){
					Log.v("file", "file not found");
					return null;
				}
				else{
					cursor=new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
					cursor.addRow(new String[]{selection,text});
					return cursor;
				}
			}
			catch (Exception e){
				e.printStackTrace();
				Log.v("file", "file not found");
			}
			Log.v("query", selection);
			return null;
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		/*String requestor=myNode.getPort();
		if(selectionArgs!=null){
			if(requestor.equals(selectionArgs[0]))
				return null;
			requestor=selectionArgs[0];
		}*/
		//search local all
		if(selection.equals("@")){
			return getKeysData();
		}
		//search all avds
		else if(selection.equals("*")){
			//only one node in ring
			/*if(myNode.getId().compareTo(myNode.getPredecessorHash())==0)
				return getKeysData();
			else{
				MatrixCursor otherAvdCursor=serachOtherAvdForAllKeys(selection,myNode.getSuccessor());
				MatrixCursor myLocalCursor=getKeysData();
				String curPairs=getKeyValuePairsFromCursor(myLocalCursor);
				String[] pairs=curPairs.isEmpty()?null:curPairs.split(";");
				if(pairs!=null){
					for(int i=0;i<pairs.length;i++){
						String[] pair=pairs[i].split(",");
						otherAvdCursor.addRow(new String[]{pair[0],pair[1]});
					}
				}
				return otherAvdCursor;
			}*/
            MatrixCursor myLocalCursor=getKeysData();
			for(String port:allNodesInRing.values()){
			    if(!port.equals(myNode.getPort())){
                    MatrixCursor tempCursor=serachOtherAvdForAllKeys("@",port);
                    String curPairs=getKeyValuePairsFromCursor(tempCursor);
                    String[] pairs=curPairs.isEmpty()?null:curPairs.split(";");
                    if(pairs!=null){
                        for(int i=0;i<pairs.length;i++){
                            String[] pair=pairs[i].split(",");
                            myLocalCursor.addRow(new String[]{pair[0],pair[1]});
                        }
                    }
                }
            }
            return myLocalCursor;
		}
		else{
			//search local
			String port=getKeyBelogingPort(selection);
			String [] sucessors=getTwoSuccessors(port);
			MatrixCursor myCursor=null;
			MatrixCursor successor1Cursor=null;
			MatrixCursor successor2Cursor=null;
			if(port.equals(myNode.getPort())){
				 myCursor=getKey(selection);
				 successor1Cursor=searchOtherAvdForKey(selection,sucessors[0]);
				 successor2Cursor=searchOtherAvdForKey(selection,sucessors[1]);

			}
			//search in othet avds
			else{
			   // String port=getKeyBelogingPort(selection);
				 myCursor=searchOtherAvdForKey(selection,port);
				 successor1Cursor=searchOtherAvdForKey(selection,sucessors[0]);
				 successor2Cursor=searchOtherAvdForKey(selection,sucessors[1]);
			}
			return myCursor!=null?myCursor:successor1Cursor!=null?successor1Cursor:successor2Cursor;
		}
	}
	private  boolean isKeyPresent(String selection){
		String [] fileList=getContext().getApplicationContext().fileList();
		Set<String> list=new HashSet<String>(Arrays.asList(fileList));
		return list.contains(selection);
	}

	private boolean deleteAllLocalFiles(){
		String files[]=getContext().getApplicationContext().fileList();
		for(String file:files){
			getContext().getApplicationContext().deleteFile(file);
		}
		return true;
	}

	private MatrixCursor searchOtherAvdForKey(String selection, String port){
		InputStream is=null;
		DataOutputStream ds=null;
		DataInputStream di=null;
		Socket socket=null;
		String msg="";
		MatrixCursor cursor=null;
		try{
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			OutputStream os = socket.getOutputStream();
			ds = new DataOutputStream(os);
			//tell leader to add this node to his ring
			ds.writeUTF("GETKEYSFORWARD,"+selection);
			try {
				is = socket.getInputStream();
				// bf = new BufferedReader(new InputStreamReader(is));
				di = new DataInputStream(is);
				//msg=bf.readLine();
				msg = di.readUTF();
				String [] split=msg.split(",");
				cursor=new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
				cursor.addRow(new String[]{split[0],split[1]});
			} catch (Exception e) {
				Log.e(TAG, "query forward failed inside");
			}
			finally {
				is.close();
				ds.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
			Log.e(TAG, "query forward failed outside");
		}
		finally {
			try {
				if(!socket.isClosed())socket.close();
			}
			catch (Exception e){
				Log.e(TAG,"Error Closing socket");
			}
		}
		Log.e(TAG, "key not found something wrong");
		return cursor;
	}

	private MatrixCursor serachOtherAvdForAllKeys(String selection,String port){
		InputStream is=null;
		DataOutputStream ds=null;
		DataInputStream di=null;
		Socket socket=null;
		String msg="";
		MatrixCursor cursor=null;
		try{
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			OutputStream os = socket.getOutputStream();
			ds = new DataOutputStream(os);
			//tell leader to add this node to his ring
			ds.writeUTF("GETKEYSFORWARD,"+selection);
			try {
				is = socket.getInputStream();
				// bf = new BufferedReader(new InputStreamReader(is));
				di = new DataInputStream(is);
				//msg=bf.readLine();
				msg = di.readUTF();
				cursor=new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
			} catch (Exception e) {
				Log.e(TAG, "query forward failed inside");
			}
			finally {
				is.close();
				ds.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
			Log.e(TAG, "query forward failed outside");
		}
		finally {
			try {
				if(!socket.isClosed())socket.close();
			}
			catch (Exception e){
				Log.e(TAG,"Error Closing socket");
			}
		}
		if(!msg.isEmpty()){
			String [] pairs=msg.split(";");
			for(int i=0;i<pairs.length;i++){
				String split[]=pairs[i].split(",");
				cursor.addRow(new String[]{split[0],split[1]});
			}
		}
		Log.e(TAG, "key not found something wrong");
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
	public String getNodeHash(String port){
		try {
			if(portSet.contains(port)){
				Integer temp=Integer.parseInt(port)/2;
				return genHash(temp.toString());
			}
			return genHash(port);
		} catch (NoSuchAlgorithmException e) {
			Log.e("Node Hash","Unable to get node Hash");
		}
		return null;
	}

	public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket client;
			InputStream is;
			String msg = "";
			DataInputStream di = null;
			DataOutputStream ds = null;
			//reference:https://docs.oracle.com/javase/7/docs/api/java/net/Socket.html
			//reference:https://docs.oracle.com/javase/7/docs/api/java/io/BufferedReader.html
			while (!serverSocket.isClosed()) {
				try {
					// serverSocket.setSoTimeout(5000);
					//server accepts from client
					client = serverSocket.accept();
					is = client.getInputStream();
					// bf = new BufferedReader(new InputStreamReader(is));
					String s = null;
					di = new DataInputStream(is);
					msg = di.readUTF();
					//split message to check msg type
					String [] msgSplit=msg.split(",");
					OutputStream os=client.getOutputStream();
					ds=new DataOutputStream(os);
//                    OutputStream os=client.getOutputStream();
//                    ds=new DataOutputStream(os);
					//if node add to ring request
					//we already checked for leader avd0,so don't worry about it.
					if(msgSplit.length==2&&msgSplit[0].equals("ADD")){
						String incomingPort=msgSplit[1];
//                        String incomingNodeHash=getNodeHash(incomingPort);
//                        String oldPredecessor=myNode.getPredecessor();
//                        String oldSucceossor=myNode.getSuccessor();
						//findSuccessor(incomingPort);
                       // Log.e(TAG, "came to add value "+incomingPort);
                      //  allNodesInRing.put(getNodeHash(incomingPort),incomingPort);
                        Log.e(TAG, "came size after  "+allNodesInRing.size());
						//only leader is live in network i.e. only one node in network
//                        if(myNode.getPredecessorHash().compareTo(myNode.getId())==0){
//                            myNode.setPredecessor(incomingPort);
//                            myNode.setSuccessor(incomingPort);
//                            ds.writeUTF(leaderPort+","+leaderPort);
//                            Log.e(TAG, "Update nodes predecessor and successor"+leaderPort+","+leaderPort);
//                        }
//                        //incoming node lies between leaders predecessor and leader
//                        else if(myNode.getPredecessorHash().compareTo(incomingNodeHash)>0&&myNode.getId().compareTo(incomingNodeHash)<=0){
//                            myNode.setPredecessor(incomingPort);
//                            //tell old predecessor to update its successor to incoming port
//                            //send updated pred and suc to node
//                            ds.writeUTF(oldPredecessor+","+leaderPort);
//                            Log.e(TAG, "Update nodes predecessor and successor"+oldPredecessor+","+leaderPort);
//
//                        }
//                        //incoming node lies between leaders successor and leader
//                        else if(myNode.getSuccessorHash().compareTo(incomingNodeHash)<=0&&myNode.getId().compareTo(incomingNodeHash)>0){
//                            myNode.setSuccessor(incomingPort);
//                            //tell old successor to update its predecessor to incoming port
//                            //send updated pred and suc to node
//                            ds.writeUTF(leaderPort+","+oldSucceossor);
//                            Log.e(TAG, "Update nodes predecessor and successor"+leaderPort+","+oldSucceossor);
//
//                        }
						//find location in ring and update predecessor and successor
//                        else{
//                            Node n=findSuccessor(incomingPort);
//                        }
					}
					//update predecessor and successor
					else if(msgSplit[0].equals("UPDATE_P_S")&&myNode.getPort().equals(msgSplit[1])){
						myNode.setPredecessor(msgSplit[2]);
						myNode.setSuccessor(msgSplit[3]);
						Log.e(TAG, myNode.toString());
					}
					else if(msgSplit[0].equals("INSERTFORWARD")) {
						ContentValues cv = new ContentValues();
						cv.put(KEY_FIELD, msgSplit[1]);
						cv.put(VALUE_FIELD, msgSplit[2]);
						//insert checks for right node else forward request to successor
						//careful about circular infinite loop
						//insert(mUri, cv);
						/*FileOutputStream outputStream=null;
                        outputStream = getContext().getApplicationContext().openFileOutput(msgSplit[1], Context.MODE_PRIVATE);
                        outputStream.write(msgSplit[2].getBytes());
                        outputStream.close();*/
						FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(msgSplit[1], Context.MODE_PRIVATE);
						outputStream.write(msgSplit[2].getBytes());
						outputStream.close();
                        Log.v("inserted forward key at", cv.toString());
					}
					else if(msgSplit[0].equals("GETKEYSFORWARD")){
                        Cursor cursor;
                        if(msgSplit[1].equals("@")){
                            cursor=getKeysData();
                        }
                        else{
                            cursor = getKey(msgSplit[1]);
                        }
						if(cursor!=null){
							String ret = getKeyValuePairsFromCursor((MatrixCursor)cursor);
							Log.e(TAG,"found key here "+ret);
							ds.writeUTF(ret);
						}
						else
							ds.writeUTF("");
                       /* Cursor cursor = query(mUri, null, msgSplit[1], null, null);
                        if(cursor!=null){
                            String ret = getKeyValuePairsFromCursor((MatrixCursor)cursor);
                            Log.e(TAG,"found key here "+ret);
                            ds.writeUTF(ret);
                        }
                        else
                            ds.writeUTF("");*/
					}
					else if(msgSplit[0].equals("DELETEFORWARD")){
                        delete(mUri, msgSplit[1], new String[]{"true"});
					    /*if(msgSplit[1].equals("*"))
						    deleteAllLocalFiles();
					    else{
                            getContext().getApplicationContext().deleteFile(msgSplit[2]);
                            Log.v("deleted forward key at", myNode.getPort());
                        }*/
					}
					else if(msgSplit[0].equals("LIVEAGAIN")){
						MatrixCursor cursor=findFailedNodeData(msgSplit[1],msgSplit[2]);
						if(cursor.getCount()!=0){
							String ret = getKeyValuePairsFromCursor((MatrixCursor)cursor);
							Log.e(TAG,"failedPort keys found  here "+ret);
							ds.writeUTF(ret);
						}
						else
							ds.writeUTF("");
						//delete(mUri, msgSplit[1], null);
					    /*if(msgSplit[1].equals("*"))
						    deleteAllLocalFiles();
					    else{
                            getContext().getApplicationContext().deleteFile(msgSplit[2]);
                            Log.v("deleted forward key at", myNode.getPort());
                        }*/
					}
				}
				catch (IOException e) {
					e.printStackTrace();
					Log.e(TAG, "Can't listen to client/issue with connection");
				}
			}
			return null;
		}

		private MatrixCursor findFailedNodeData(String failedPort,String predessor) {
				// TODO Auto-generated method stub
				MatrixCursor cursor=new MatrixCursor(new String[]{KEY_FIELD,VALUE_FIELD});
				String text="";
				String [] fileList=getContext().getApplicationContext().fileList();
//				for(String s:fileList)
//					Log.v("vishal files ", s);
				for(String selection:fileList) {
					//this is wrong
					if(isRightNode(getNodeHash(selection),failedPort,predessor)){
						try {
							// BufferedReader br=new BufferedReader(new FileReader(selection));
							FileInputStream fis = getContext().getApplicationContext().openFileInput(selection);
							text="";
							int value = 0;
							while ((value = fis.read()) != -1) {
								text += (char) value;
							}
							if (text.equals("")) {
								Log.v("file", "file not found");
								//return null;
							} else {
								cursor.addRow(new String[]{selection, text});
							}
						} catch (Exception e) {
							e.printStackTrace();
							Log.v("file", "file not found");
						}
					}
				}
				Log.v("query", "successfully found rows "+cursor.getCount());
				return cursor;
		}

		/*private void findSuccessor(String incomingPort) {
			allNodesInRing.put(getNodeHash(incomingPort),incomingPort);
			String arr[]= new String[allNodesInRing.size()];
			int index=0;
			for(String s:allNodesInRing.values()){
				arr[index++]=s;
			}
			int length=arr.length;
			for(int i=1;i<arr.length;i++){
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "UPDATE", arr[i%length],arr[(i-1)%length],arr[(i+1)%length]);
			}
			if(arr.length>1){
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "UPDATE",arr[0],arr[length-1],arr[1]);
			}
		}*/

		protected void onProgressUpdate(String...strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */

			/*
			 * The following code creates a file in the AVD's internal storage and stores a file.
			 *
			 * For more information on file I/O on Android, please take a look at
			 * http://developer.android.com/training/basics/data-storage/files.html
			 */

			return;
		}
	}

	private String getKeyValuePairsFromCursor(MatrixCursor cursor) {
		String pairs="";
		if(cursor==null)
			return pairs;
		int keyIndex=cursor.getColumnIndex(KEY_FIELD);
		int valIndex=cursor.getColumnIndex(VALUE_FIELD);
		try {
			for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
				String key=cursor.getString(keyIndex);
				String val=cursor.getString(valIndex);
				pairs+=key+","+val+";";
			}
		} finally {
			cursor.close();
		}
		return pairs.equals("")?pairs:pairs.substring(0,pairs.length()-1);
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		Socket socket;
		DataOutputStream ds=null;
		DataInputStream di=null;
		@Override
		protected Void doInBackground(String... msgs) {
			try {
				//send join request to avd0 node
				if (msgs[0].equals("JOIN")) {
					String [] fileList=getContext().getApplicationContext().fileList();
					InputStream is = null;
					for (String port : remotePort)
						allNodesInRing.put(getNodeHash(port), port);
					//recover mode
					if(fileList.length!=0) {
//						InputStream is = null;
//						for (String port : remotePort)
//							allNodesInRing.put(getNodeHash(port), port);
						String[] successors = getTwoSuccessors(myNode.getPort());
						String predessor = remotePort[4];
						for (String port : allNodesInRing.values()) {
							if (port.equals(myNode.getPort()))
								break;
							predessor = port;
						}
						String predessorpred = remotePort[4];
						for (String port : allNodesInRing.values()) {
							if (port.equals(predessor))
								break;
							predessorpred = port;
						}
						String predessorpredpred = remotePort[4];
						for (String port : allNodesInRing.values()) {
							if (port.equals(predessorpred))
								break;
							predessorpredpred = port;
						}
						String pairs1[]=null;
						String pairs2[]=null;
						String pairsp1[]=null;
						String pairsp2[]=null;
						//successor 1
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(successors[0]));
						OutputStream os = socket.getOutputStream();
						ds = new DataOutputStream(os);
						//tell leader to add this node to his ring
						ds.writeUTF("LIVEAGAIN," + myNode.getPort() + "," + predessor);
						try {
							is = socket.getInputStream();
							// bf = new BufferedReader(new InputStreamReader(is));
							di = new DataInputStream(is);
							//msg=bf.readLine();
							String msg = di.readUTF();
							if (!msg.trim().isEmpty()) {
								pairs1 = msg.split(";");
								/*for (int i = 0; i < pairs.length; i++) {
									splits1 = pairs[i].split(",");
									FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
									outputStream.write(split[1].getBytes());
									outputStream.close();
								}
								Log.e(TAG, "succefully inserted all recovered data from " + successors);*/
							}
						} catch (Exception e) {
							Log.e(TAG, "failed to  inserted all recovered data");
						} finally {
							if (is != null) is.close();
							ds.close();
						}
						//successor 2
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(successors[1]));
						OutputStream os3 = socket.getOutputStream();
						ds = new DataOutputStream(os3);
						//tell leader to add this node to his ring
						ds.writeUTF("LIVEAGAIN," + myNode.getPort() + "," + predessor);
						try {
							is = socket.getInputStream();
							// bf = new BufferedReader(new InputStreamReader(is));
							di = new DataInputStream(is);
							//msg=bf.readLine();
							String msg = di.readUTF();
							if (!msg.trim().isEmpty()) {
								pairs2 = msg.split(";");
								/*for (int i = 0; i < pairs.length; i++) {
									splits1 = pairs[i].split(",");
									FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
									outputStream.write(split[1].getBytes());
									outputStream.close();
								}
								Log.e(TAG, "succefully inserted all recovered data from " + successors);*/
							}
						} catch (Exception e) {
							Log.e(TAG, "failed to  inserted all recovered data");
						} finally {
							if (is != null) is.close();
							ds.close();
						}
						//predecessor 1
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(predessor));
						OutputStream os1 = socket.getOutputStream();
						ds = new DataOutputStream(os1);
						//tell leader to add this node to his ring
						ds.writeUTF("LIVEAGAIN," + predessor + "," + predessorpred);
						try {
							is = socket.getInputStream();
							// bf = new BufferedReader(new InputStreamReader(is));
							di = new DataInputStream(is);
							//msg=bf.readLine();
							String msg = di.readUTF();
							if (!msg.trim().isEmpty()) {
								pairsp1 = msg.split(";");
								/*for (int i = 0; i < pairs.length; i++) {
									String split[] = pairs[i].split(",");
									FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
									outputStream.write(split[1].getBytes());
									outputStream.close();
								}
								Log.e(TAG, "succefully inserted all rata fropm " + predessor);*/
							}
						} catch (Exception e) {
							Log.e(TAG, "failed to  inserted all recovered data");
						} finally {
							if (is != null) is.close();
							ds.close();
						}
						//predecessor 2
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(predessorpred));
						OutputStream os2 = socket.getOutputStream();
						ds = new DataOutputStream(os2);
						//tell leader to add this node to his ring
						ds.writeUTF("LIVEAGAIN," + predessorpred + "," + predessorpredpred);
						try {
							is = socket.getInputStream();
							// bf = new BufferedReader(new InputStreamReader(is));
							di = new DataInputStream(is);
							//msg=bf.readLine();
							String msg = di.readUTF();
							if (!msg.trim().isEmpty()) {
								pairsp2 = msg.split(";");
								/*for (int i = 0; i < pairs.length; i++) {
									String split[] = pairs[i].split(",");
									FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
									outputStream.write(split[1].getBytes());
									outputStream.close();
								}
								Log.e(TAG, "succefully inserted all recovered data fropm " + predessorpred);*/
							}
						} catch (Exception e) {
							Log.e(TAG, "failed to  inserted all recovered data");
						} finally {
							if (is != null) is.close();
							ds.close();
						}
						if(pairs1!=null)
						Log.e(TAG, "size of pairs1 "+pairs1.length);
						if(pairs2!=null)
						Log.e(TAG, "size of pairs2 "+pairs2.length);
						if(pairsp1!=null)
						Log.e(TAG, "size of pairsp1 "+pairsp1.length);
						if(pairsp2!=null)
						Log.e(TAG, "size of pairsp2 "+pairsp2.length);
						//insert values
						for (int i = 0; i < pairs1.length; i++) {
							String split[] = pairs1[i].split(",");
							FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
							outputStream.write(split[1].getBytes());
							outputStream.close();
						}
						for (int i = 0; i < pairs2.length; i++) {
							String split[] = pairs2[i].split(",");
							FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
							outputStream.write(split[1].getBytes());
							outputStream.close();
						}
						for (int i = 0; i < pairsp1.length; i++) {
							String split[] = pairsp1[i].split(",");
							FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
							outputStream.write(split[1].getBytes());
							outputStream.close();
						}
						for (int i = 0; i < pairsp2.length; i++) {
							String split[] = pairsp2[i].split(",");
							FileOutputStream outputStream = getContext().getApplicationContext().openFileOutput(split[0], Context.MODE_PRIVATE);
							outputStream.write(split[1].getBytes());
							outputStream.close();
						}
//				    makeRing();
					/*for(String port:remotePort){
					    if(!port.equals(myNode.getPort())){
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(port));
                            OutputStream os = socket.getOutputStream();
                            ds = new DataOutputStream(os);
                            //tell leader to add this node to his ring
                            ds.writeUTF("ADD,"+msgs[1]);
                        }
                    }*/
//                    is = socket.getInputStream();
//                    // bf = new BufferedReader(new InputStreamReader(is));
//                    di = new DataInputStream(is);
//                    //msg=bf.readLine();
//                    //update predecessor and successor told by avd0
//                    String [] msg = di.readUTF().split(",");
//                    myNode.setPredecessor(msg[0]);
//                    myNode.setSuccessor(msg[1]);
//
//                    //now tell predecessor and successor to update their nodes
					}

				}
				if(msgs[0].equals("UPDATE")){
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(msgs[1]));
					OutputStream os = socket.getOutputStream();
					ds = new DataOutputStream(os);
					//tell nodes to update their predecessor and successor in ring
					ds.writeUTF("UPDATE_P_S,"+msgs[1]+","+msgs[2]+","+msgs[3]);
					Log.e(TAG, "updating s n p of node "+msgs[1]+","+msgs[2]+","+msgs[3]);
				}
				if(msgs[0].equals("INSERT")){
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(msgs[1]));
					OutputStream os = socket.getOutputStream();
					ds = new DataOutputStream(os);
					ds.writeUTF("INSERTFORWARD,"+msgs[2]+","+msgs[3]);
				}
				if(msgs[0].equals("DELETE")){
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(msgs[1]));
					OutputStream os = socket.getOutputStream();
					ds = new DataOutputStream(os);
					ds.writeUTF("DELETEFORWARD,"+msgs[2]);
				}
			}
			catch (Exception e){
				Log.e(TAG, "ClientTask socket Exception for client "+msgs[1]);
			}
			finally {
				try {
					if(ds!=null) ds.close();
					if(!socket.isClosed())socket.close();
				}
				catch (Exception e){
					Log.e(TAG,"Error Closing socket");
				}
			}
			return null;
		}

		private String [] getTwoSuccessors(String port){
			String arr[]= new String[allNodesInRing.size()];
			int index=0;
			for(String s:allNodesInRing.values()){
				arr[index++]=s;
			}
			int length=arr.length;
			String [] ret=new String[2];
			for(int i=0;i<arr.length;i++){
				if(arr[i].equals(port)){
					ret[0]=arr[((i+1)%length)];
					ret[1]=arr[((i+2)%length)];
				}
			}
			return ret;
		}

	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private  void makeRing(){
        for(String port:remotePort)
          allNodesInRing.put(getNodeHash(port),port);
    }

}
