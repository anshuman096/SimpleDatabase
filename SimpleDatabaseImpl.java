package org.simpledatabase;
import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;

public class SimpleDatabaseImpl implements SimpleDatabase {
	private static SimpleDatabaseImpl database = new SimpleDatabaseImpl();
	private Stack<TransactionBlock> blockStack = new Stack<TransactionBlock> ();
	private HashMap<String, ValueTuple> cache = new HashMap<String, ValueTuple> ();
	private HashMap<String, ValueTuple> globalUncommitedRemovals = new HashMap<String, ValueTuple> ();
    private HashMap<String, ValueTuple> localUncommitedRemovals = new HashMap<String, ValueTuple> ();
	
	private class TransactionBlock{
        private HashMap <String, ValueTuple> dataset = new HashMap<String, ValueTuple>();
		private TransactionBlock () {} 
	}
	
	private class ValueTuple {
		Integer value;
		Date ts; 
		public ValueTuple(Integer v) {
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
	public void set(String key, Integer value) {
        ValueTuple v = new ValueTuple(value);
        if(database.blockStack.isEmpty()) {
            database.cache.put(key, v);    
        } else
            database.blockStack.peek().dataset.put(key, v);
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#get(java.lang.String)
	 */
	@Override
	public void get(String key) {
		//first search for variable in global cache
		ValueTuple result = null;
		if(database.cache.containsKey(key)) 
			result = database.cache.get(key);
		//search through the current transaction blocks next
        if(!database.blockStack.isEmpty()) {
        	TransactionBlock current = database.blockStack.peek();
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
	public void unset(String key) {
    	ValueTuple v = null;
	        if(database.cache.containsKey(key)) { //globally remove
        	    v = database.cache.remove(key);
	            database.globalUncommitedRemovals.put(key, v);
        	    return;
        	}
        	if(!database.blockStack.isEmpty()) { //remove within transaction blocks
	            if(database.blockStack.peek().dataset.containsKey(key)) {
        	        v = database.blockStack.peek().dataset.remove(key);
               		database.localUncommitedRemovals.put(key, v);
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
	public int numEqualTo(Integer value) {
		int counter = 0;
        for(String key : database.cache.keySet()) { //global check
            if(value.equals(database.cache.get(key).value))
                counter++;
        }
        if(!database.blockStack.isEmpty()) {
    		for (String key : database.blockStack.peek().dataset.keySet()) { //transaction block check
                if (value.equals(database.blockStack.peek().dataset.get(key).value))
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
		database.blockStack.push(t);
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#rollBack()
	 */
	@Override
	public void rollBack() {
		if(database.blockStack.size() == 0)
			System.out.println("> NO TRANSACTION");
		else {
			database.blockStack.pop();
            if(!database.blockStack.isEmpty()) {
                database.cache.putAll(database.globalUncommitedRemovals);
                database.blockStack.peek().dataset.putAll(database.localUncommitedRemovals);
                database.globalUncommitedRemovals.clear();
                database.localUncommitedRemovals.clear();
            }
        }
		
	}
	
	/* (non-Javadoc)
	 * @see org.simpledatabase.SimpleDabatase#commit()
	 */
	@Override
	public void commit() {
        if(database.blockStack.size() == 0) {
            System.out.println("> NO TRANSACTION");
            return;
        } else {
            Stack<TransactionBlock> temp = new Stack<TransactionBlock> ();
            while(!database.blockStack.isEmpty()) 
                temp.push(database.blockStack.pop()); // in order to preserve order in which variables were entered
            while(!temp.isEmpty()) 
                database.cache.putAll(temp.pop().dataset);
        }
	}
	
	
	public static void main(String[] args) {
		SimpleDatabase database = SimpleDatabaseImpl.getInstance();
		Scanner sc = new Scanner(System.in);
		while(sc.hasNextLine()) {
			String command = sc.nextLine();
			System.out.println(command);
			String[] tokens = command.split(" ");
			String command_name = tokens[0];
			String var_name;
			Integer value;
			switch(command_name) {
			case "SET":
				var_name = tokens[1];
				value = Integer.valueOf(tokens[2]);
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
				value = Integer.valueOf(tokens[1]);
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
