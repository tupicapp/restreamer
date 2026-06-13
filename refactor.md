the big mistake of hierarchial design in streaming is that by any change you should go from top to down and changing the encoder when its running is not the best option if of course it is our last layer of engine.
the best option is to remove the first layer and see it left to right so by having no input it wil decide what to do automatically.

source should always leading the time of encoder since encoder buffers need some preset packets so by this way we can show our user results very fast so sources are push-based with buffers.
being push based means that users of it doesnt effect its behavior and it has its own buffer always ready to server and by pulling it doesnt change its behavior so its push-based.

the engine should remove left to right and atomically so later layers will show something to the user even if they have no good input.

components has multi inputs and multi outputs.
their inputs are equal to each we do an operation and then.
the outputs are same also.

we have some component types and we can add custom components with the lifecycle implicitly managed by the endigne

close mechanisms : if a left component removed we remove it and remove next layers if they have reached 0 input because of this removation so removes first remove the component and then if next layers is 0 we remove them and we dont touch former layers and will remove that input it they became 0 output.
so first the component then goes to right. and then goes to left.

start mechanisms : starts get cascaded right to left so for example (first destinations then encoder then other layers)

every engine has a manifest that we can get its manifest.

every component has its own manager and the engine itself doesnt have any seperated manager and if we want to manage the engine we should do it from outside.