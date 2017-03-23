package org.simpledatabase;
import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;

public class SimpleDatabaseImpl<K,V> implements SimpleDatabase<K,V> {
	private static SimpleDatabase database = new SimpleDatabaseImpl();
	private Stack<TransactionBlock> blockStack = new Stack<TransactionBlock> ();
	private HashMap<K, ValueTuple> cache = new HashMap<K, ValueTuple> ();
	private HashMap<K, ValueTuple> globalUncommitedRemovals = new HashMap<K, ValueTuple> ();
    private HashMap<K, ValueTuple> localUncommitedRemovals = new HashMap<K, ValueTuple> ();
	
	private class TransactionBlock{
		private HashMap <K, ValueTuple> dataset = new HashMap<K, ValueTuple>();
		private TransactionBlock () {} 
	}
	
	class ValueTuple {
		V value;
		Date ts; 
		public ValueTuple(V v) {
			value = v;
			ts = new Date();
		}
	}
	
	public static SimpleDatabase getInstance() {
		return database;
	}
	
	private SimpleDatabaseImpl () {
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#set(java.lang.String, java.lang.Integer)
	 */
	@Override
	public void set(K key, V value) {
        ValueTuple v = new ValueTuple(value);
        if(blockStack.isEmpty()) {
            cache.put(key, v);    
        } else
            blockStack.peek().dataset.put(key, v);
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#get(java.lang.String)
	 */
	@Override
	public void get(K key) {
		//first search for variable in global cache
		ValueTuple result = null;
		if(cache.containsKey(key)) 
			result = cache.get(key);
		//search through the current transaction blocks next
        if(!blockStack.isEmpty()) {
        	TransactionBlock current = blockStack.peek();
	        if(current.dataset.containsKey(key)) {
        	    ValueTuple tVal = current.dataset.get(key);
	        	if(result != null) {
        	     	if(tVal.ts.getTime() > result.ts.getTime())
                	    result = tVal;
	            } else
        	        result = tVal;
            }
        }
		if(result != null)
			System.out.println("> " + result.value.toString());
		else
			System.out.println("> NULL");
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#unset(java.lang.String)
	 */
	@Override
	public void unset(K key) {
    	ValueTuple v = null;
	        if(cache.containsKey(key)) { //globally remove
        	    v = cache.remove(key);
	            globalUncommitedRemovals.put(key, v);
        	    return;
        	}
        	if(!blockStack.isEmpty()) { //remove within transaction blocks
	            if(blockStack.peek().dataset.containsKey(key)) {
        	        v = blockStack.peek().dataset.remove(key);
               		localUncommitedRemovals.put(key, v);
	                return;
           	    }
        	}
	        System.out.println("> NULL");
	}
	
	/**
	 * Numequalto checks just the transaction blocks 
	 * and returns the number of variables that have the same value as 
	 * the parameter passed in.
	 * 
	 * @param value: the value we are searching for
	 * @return : The number of variables in both global and transaction storage with same value
	 */
	public int numEqualTo(V value) {
		int counter = 0;
        for(K key : cache.keySet()) { //global check
            if(value.equals(cache.get(key).value))
                counter++;
        }
        if(!blockStack.isEmpty()) {
    		for (K key : blockStack.peek().dataset.keySet()) { //transaction block check
                if (value.equals(blockStack.peek().dataset.get(key).value))
                    counter++;
		    }
        }
        return counter;
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#begin()
	 */
	@Override
	public void begin() {
		TransactionBlock t = new TransactionBlock();
		blockStack.push(t);
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#rollBack()
	 */
	@Override
	public void rollBack() {
		if(blockStack.size() == 0)
			System.out.println("> NO TRANSACTION");
		else {
			blockStack.pop();
            if(!blockStack.isEmpty()) {
                cache.putAll(globalUncommitedRemovals);
                blockStack.peek().dataset.putAll(localUncommitedRemovals);
                globalUncommitedRemovals.clear();
                localUncommitedRemovals.clear();
            }
        }
		
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#commit()
	 */
	@Override
	public void commit() {
        if(blockStack.size() == 0) {
            System.out.println("> NO TRANSACTION");
            return;
        } else {
            Stack<TransactionBlock> temp = new Stack<TransactionBlock> ();
            while(!blockStack.isEmpty()) 
                temp.push(blockStack.pop()); // in order to preserve order in which variables were entered
            while(!temp.isEmpty()) 
                cache.putAll(temp.pop().dataset);
        }
	}
	
	
	public static void main(String[] args) {
		SimpleDatabase<String,String> database = SimpleDatabaseImpl.getInstance();
		Scanner sc = new Scanner(System.in);
		while(sc.hasNextLine()) {
			String command = sc.nextLine();
			System.out.println(command);
			String[] tokens = command.split(" ");
			String command_name = tokens[0];
			String var_name;
			String value;
			switch(command_name) {
			case "SET":
				var_name = tokens[1];
				//value = Integer.valueOf(tokens[2]);
				value = tokens[2];
				database.set(var_name, value);
				break;
			case "GET":
				var_name = tokens[1];
				database.get(var_name);
				break;
			case "UNSET":
				var_name = tokens[1];
				database.unset(var_name);
				break;
			case "NUMEQUALTO":
				//value = Integer.valueOf(tokens[1]);
				value = tokens[1];
				int ctr = database.numEqualTo(value);
				System.out.println("> " + ctr);
				break;
			case "BEGIN":
				database.begin();
				break;
			case "ROLLBACK":
				database.rollBack();
				break;
			case "COMMIT":
				database.commit();
				break;
			case "END": 
				sc.close();
				return;
			default:
				System.out.println("Invalid command");
			}
			
		}
		sc.close();
	}
	
}
