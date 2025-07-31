import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.pilot.State;

public class TestStateShallowCopy {
    //test State.shalllowCopy for List
    @Test
    public void testShallowCopy() {
        ImmutableList list1 = ImmutableList.of("a", "b", "c");
        ImmutableList list2 = ImmutableList.of("d", "e", "f", "g");
        ImmutableList shallowCopy = State.shallowCopy(list1, list2, true);
        assert shallowCopy.size() == 4 : "Shallow copy should contain 6 elements";

    }
}
