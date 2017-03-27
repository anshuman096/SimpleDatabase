import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.simpledatabase.SimpleDatabase;
import org.simpledatabase.SimpleDatabaseImpl;

import junit.framework.TestCase;
/**
 * 
 */

/**
 * @author anshuman
 *
 */
public class SimpleDatabaseTest extends TestCase {
	SimpleDatabase<String, String> cache;
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		cache = SimpleDatabaseImpl.getInstance();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

		
	public void testGetFromEmpty() {
		assertNull(cache.get("xx"));
	}
	
	public void testSetWithoutCommit() {
		cache.set("xx", "10");
		assertEquals("10", cache.get("xx"));
	}
	
	public void testSetWithCommit() {
		cache.commit();
		assertEquals("10", cache.get("xx"));
	}
	
	public void testSetAfterCommit() {
		cache.set("xx", "20");
		assertEquals("20", cache.get("xx"));
	}

}
