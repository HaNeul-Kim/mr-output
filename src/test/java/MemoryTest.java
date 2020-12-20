import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public class MemoryTest {

  private static final Logger logger = LoggerFactory.getLogger(MemoryTest.class);

  @Test
  public void memory() {
    int mapMemory = 2048;
    logger.debug("mapMemory = {}", mapMemory);
    String mapJavaOpts = "-Xmx" + (int) (mapMemory * 0.8) + "m";
    logger.debug("mapJavaOpts = {}", mapJavaOpts);
  }
}
