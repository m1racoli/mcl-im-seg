function h = rec_frame3(ss0, frame)

h = surf(ss0(frame).X, ss0(frame).Y, -ss0(frame).Z,'EdgeColor','none');

end