package io.s4.core.gen;

import io.s4.base.Event;
import io.s4.core.ProcessingElement;

/**
 * This interface defines methods for dispatching input and output events to the
 * most relevant methods of the processing elements.
 * 
 * <p>
 * <code>processInputEvent</code> and <code>processOutputEvent</code> methods
 * may be overloaded with different subtypes of {@link Event Event} parameters.
 * Methods defined in this interface dispatch an event to the method
 * which is the best match according to the runtime type of the event.
 * </p>
 * <p>Example: consider PE of type <code>ExamplePE extends ProcessingElement</code> that defines methods:
 * <ul>
 * <li><code>onEvent(EventA event)</code></li> 
 * <li><code>onEvent(EventB event)</code></li>
 * </ul>
 * </p>
 * <p>Then:
 * <ul>
 * <li>When event1 of type <code>EventA extends Event</code> is received on this PE, it will be handled by method <code>onEvent(EventA event)</code></li>
 * <li>When event2 of type <code>EventB extends EventA</code> is received on this PE, it will be handled by method <code>onEvent(EventB event)</code></li>
 * </ul>
 * </p>
 * <p>
 * Implementations of this interface are typically generated at runtime.
 * </p>
 */
public interface OverloadDispatcher {

    public void dispatchEvent(ProcessingElement pe, Event event);

    public void dispatchTrigger(ProcessingElement pe, Event event);
}
