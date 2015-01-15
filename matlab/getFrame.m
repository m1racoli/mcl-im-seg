function [X Y Z I] = getFrame(ss0,i)

[X Y Z I] = [ss0(i).X ss0(i).Y ss0(i).Z ss0(i).intenSR];

end